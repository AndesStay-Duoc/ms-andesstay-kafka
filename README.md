# ms-andesstay-kafka

Microservicio **administrador de la plataforma Kafka** del sistema AndesStay. Expone una API REST
para crear, modificar y eliminar tópicos y sus configuraciones, y para inspeccionar los grupos de
consumidores: miembros, offsets, **lag** y distribución del consumo.

Es un servicio de administración, no de dominio: no consume ni publica eventos de negocio y no
declara la topología de AndesStay. Los tópicos del sistema los crea `infra/kafka/compose.yml` al
levantar el clúster, y quienes producen y consumen de verdad son `ms-andesstay-reservations` y
`ms-andesstay-audit`. Lo que este servicio aporta es una vía autorizada y auditable para operar el
clúster sin entrar por `kafka-topics.sh` ni por Kafka UI.

Es el equivalente de [`ms-andesstay-mq`](https://github.com/AndesStay-Duoc/ms-andesstay-mq) para
el otro broker del caso, y sigue sus mismas convenciones.

## Una sola vía hacia el clúster

Al contrario que `ms-andesstay-mq`, que necesita dos puertos —AMQP para escribir y la API de
gestión para listar—, aquí **todo pasa por el `AdminClient`** de `kafka-clients`, que habla el
protocolo binario de los propios brokers:

| Operación | Llamada del AdminClient |
|---|---|
| Crear un tópico | `createTopics` |
| Listar y describir | `listTopics`, `describeTopics`, `describeConfigs` |
| Modificar configuraciones | `incrementalAlterConfigs` |
| Aumentar particiones | `createPartitions` |
| Eliminar un tópico | `deleteTopics` |
| Listar y describir grupos | `listConsumerGroups`, `describeConsumerGroups` |
| Offsets confirmados y finales | `listConsumerGroupOffsets`, `listOffsets` |
| Eliminar y reposicionar grupos | `deleteConsumerGroups`, `alterConsumerGroupOffsets` |

El `AdminClient` conecta de forma **perezosa**: construirlo no abre conexión. Por eso el servicio
arranca aunque el clúster esté caído y el fallo aparece como un `503` en la primera petición, que
es lo que se quiere en un administrador: tiene que poder arrancar para poder diagnosticar.

Toda la conversación con el clúster vive en
[`broker/KafkaAdminClient.java`](src/main/java/cl/andesstay/kafka/broker/KafkaAdminClient.java),
que es el único archivo del servicio que conoce tipos de `org.apache.kafka`.

## Rutas

Todas exigen un JWT de Entra ID con el **rol `Admin`**.

| Verbo | Ruta | Códigos |
|---|---|---|
| `POST` | `/api/v1/kafka/topics` | `201` `400` `401` `403` `409` `502` `503` |
| `GET` | `/api/v1/kafka/topics` | `200` `401` `403` `502` `503` |
| `GET` | `/api/v1/kafka/topics/{nombre}` | `200` `401` `403` `404` `503` |
| `PUT` | `/api/v1/kafka/topics/{nombre}/config` | `200` `400` `401` `403` `404` `503` |
| `POST` | `/api/v1/kafka/topics/{nombre}/particiones` | `200` `400` `401` `403` `404` `503` |
| `DELETE` | `/api/v1/kafka/topics/{nombre}` | `204` `401` `403` `404` `409` `503` |
| `GET` | `/api/v1/kafka/consumer-groups` | `200` `401` `403` `503` |
| `GET` | `/api/v1/kafka/consumer-groups/{id}` | `200` `401` `403` `404` `503` |
| `DELETE` | `/api/v1/kafka/consumer-groups/{id}` | `204` `401` `403` `404` `409` `503` |
| `POST` | `/api/v1/kafka/consumer-groups/{id}/offsets/reset` | `200` `400` `401` `403` `404` `409` `503` |
| `POST` | `/api/v1/kafka/pruebas/carga` | `200` `400` `401` `403` `503` · **solo con el perfil `demo`** |

Los ejemplos de abajo usan `$TOKEN` como el token de Entra ID y el servicio en
`http://localhost:8087`.

### Crear un tópico

Las particiones y el factor de replicación son opcionales: si se omiten valen **3 y 3**, que es lo
que usan los tópicos del caso. El mapa `configuraciones` se pasa tal cual al clúster.

```bash
curl -s -X POST http://localhost:8087/api/v1/kafka/topics \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"nombre":"pruebas.carga","particiones":3,"factorDeReplicacion":3,
       "configuraciones":{"retention.ms":"3600000","cleanup.policy":"delete"}}'
```

```json
{"recurso":"tópico","nombre":"pruebas.carga","operacion":"creado",
 "detalle":"creado con 3 partición(es) y factor de replicación 3; configuraciones [retention.ms, cleanup.policy]"}
```

**Crear no es idempotente.** Repetir la petición responde `409 TOPICO_YA_EXISTE` aunque las
particiones y la configuración coincidan: es lo que hace `createTopics`, y es una diferencia real
con `ms-andesstay-mq`, donde redeclarar una cola idéntica vuelve a responder bien.

### Listar los tópicos

```bash
curl -s http://localhost:8087/api/v1/kafka/topics -H "Authorization: Bearer $TOKEN"
```

```json
[{"nombre":"audit.timeline","interno":false,"protegido":true,"particiones":3,
  "factorDeReplicacion":3,"replicasEnSincronia":3,"degradado":false},
 {"nombre":"reservations.events","interno":false,"protegido":true,"particiones":3,
  "factorDeReplicacion":3,"replicasEnSincronia":2,"degradado":true}]
```

`replicasEnSincronia` es el **mínimo** de réplicas al día entre las particiones del tópico, y
`degradado` avisa de que alguna partición tiene menos réplicas al día que réplicas, o se quedó sin
líder. Es el número que importa: con `min.insync.replicas=2`, que es lo que tiene el clúster del
caso, basta con que una partición baje de dos réplicas al día para que las publicaciones con
`acks=all` a esa partición empiecen a fallar.

Los tópicos internos se esconden. Con `?incluirInternos=true` aparecen también:

```bash
curl -s 'http://localhost:8087/api/v1/kafka/topics?incluirInternos=true' \
  -H "Authorization: Bearer $TOKEN"
```

| Se esconde | Por qué |
|---|---|
| `__consumer_offsets`, `__transaction_state` y todo lo que empiece por `__` | Son del clúster; tocarlos lo rompe. `__consumer_offsets` tiene cincuenta particiones él solo |
| `_schemas` | Es donde el Schema Registry guarda los esquemas: infraestructura, no un tópico de negocio |
| Lo que termine en `-changelog` o `-repartition` | Son los almacenes de estado y las particiones intermedias que crea Kafka Streams. Para el clúster son tópicos normales: solo el nombre los distingue |

### Detalle de un tópico

```bash
curl -s http://localhost:8087/api/v1/kafka/topics/reservations.events \
  -H "Authorization: Bearer $TOKEN"
```

```json
{"nombre":"reservations.events","interno":false,"protegido":true,"particiones":3,
 "factorDeReplicacion":3,"degradado":false,
 "detalleDeParticiones":[
   {"particion":0,
    "lider":{"id":1,"host":"kafka1","puerto":29092},
    "replicas":[{"id":1,"host":"kafka1","puerto":29092},{"id":2,"host":"kafka2","puerto":29093},
                {"id":3,"host":"kafka3","puerto":29094}],
    "isr":[{"id":1,"host":"kafka1","puerto":29092},{"id":2,"host":"kafka2","puerto":29093},
           {"id":3,"host":"kafka3","puerto":29094}],
    "enSincronia":true}],
 "configuraciones":{"cleanup.policy":"delete","retention.ms":"604800000","…":"…"},
 "configuracionesPersonalizadas":{"retention.ms":"604800000"}}
```

Las configuraciones vienen **dos veces** y no es redundancia: `configuraciones` son las del orden
de sesenta claves efectivas, con el valor que el clúster aplica de verdad, y
`configuracionesPersonalizadas` solo las fijadas sobre ese tópico. La primera responde «y entonces
cuánto retiene»; la segunda, «qué distingue a este tópico de los demás».

### Modificar configuraciones

```bash
curl -s -X PUT http://localhost:8087/api/v1/kafka/topics/reservations.events/config \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"configuraciones":{"retention.ms":"604800000","min.insync.replicas":"2"}}'
```

El cambio es **incremental**: solo se tocan las claves enviadas y el resto queda como estaba. La
alternativa de Kafka, `alterConfigs` a secas, reemplaza la configuración completa del recurso, de
modo que enviar solo `retention.ms` devolvería a su valor por omisión todo lo demás que estuviera
puesto. Esta API no la usa nunca.

Un valor `null` **quita** la clave y la devuelve a su valor por omisión. Es la única forma de
quitar una configuración en lugar de fijarla:

```bash
curl -s -X PUT http://localhost:8087/api/v1/kafka/topics/pruebas.carga/config \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"configuraciones":{"retention.ms":null}}'
```

### Aumentar particiones

El número es el **total** que tendrá el tópico, no cuántas se agregan.

```bash
curl -s -X POST http://localhost:8087/api/v1/kafka/topics/pruebas.carga/particiones \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"particiones":6}'
```

```json
{"recurso":"tópico","nombre":"pruebas.carga","operacion":"reparticionado",
 "detalle":"pasó de 3 a 6 particiones. El reparto por clave cambia desde ahora: una misma clave puede caer en otra partición, así que el orden por clave no se mantiene respecto de los mensajes anteriores"}
```

Pedir un total **menor o igual** al actual responde `400` con la explicación: Kafka no implementa
la bajada, porque los mensajes ya publicados están repartidos por el resto de la clave entre las
particiones que hay y quitar una dejaría sus mensajes sin sitio. Para bajarlas hay que crear otro
tópico y migrar.

### Eliminar un tópico

```bash
curl -s -X DELETE http://localhost:8087/api/v1/kafka/topics/pruebas.carga \
  -H "Authorization: Bearer $TOKEN" -i
```

`204` sin cuerpo. Sobre un tópico de la topología responde `409`; sobre uno inexistente, `404`.

### Listar los grupos de consumidores

```bash
curl -s http://localhost:8087/api/v1/kafka/consumer-groups -H "Authorization: Bearer $TOKEN"
```

```json
[{"id":"andesstay-audit","estado":"Stable","simple":false},
 {"id":"andesstay-report","estado":"Empty","simple":false}]
```

Los estados son los de Kafka, escritos como los escriben su consola y Kafka UI: `Stable` es el
grupo consumiendo con normalidad, `Empty` uno sin miembros pero con offsets guardados,
`PreparingRebalance` y `CompletingRebalance` los dos momentos de un rebalanceo, y `Dead` un grupo
que el clúster ya no conoce.

### Detalle de un grupo, con el lag

Es la consulta que resume de qué va este servicio.

```bash
curl -s http://localhost:8087/api/v1/kafka/consumer-groups/andesstay-audit \
  -H "Authorization: Bearer $TOKEN"
```

```json
{"id":"andesstay-audit","estado":"Stable","simple":false,
 "coordinador":{"id":1,"host":"kafka1","puerto":29092},
 "estrategiaDeAsignacion":"range",
 "miembros":[{"id":"consumidor-1","cliente":"ms-andesstay-audit","host":"10.0.0.5",
              "asignacion":["reservations.events-0","reservations.events-1"]}],
 "particiones":[
   {"topico":"reservations.events","particion":0,"offsetConfirmado":1200,"offsetFinal":1250,
    "lag":50,"miembro":"ms-andesstay-audit"},
   {"topico":"reservations.events","particion":1,"offsetConfirmado":900,"offsetFinal":1600,
    "lag":700,"miembro":"ms-andesstay-audit"},
   {"topico":"reservations.events","particion":2,"offsetConfirmado":300,"offsetFinal":1000,
    "lag":700,"miembro":null}],
 "distribucion":[
   {"miembro":"ms-andesstay-audit","host":"10.0.0.5","particiones":2,"lag":750},
   {"miembro":null,"particiones":1,"lag":700}],
 "lagTotal":1450,
 "particionesSinConfirmar":0,
 "particionesSinAsignar":1}
```

**El lag no lo calcula Kafka.** No hay ninguna operación del `AdminClient` que lo devuelva: lo que
hay son dos lecturas separadas —el offset que el grupo confirmó y el offset final de la
partición— y la resta la hace quien quiera saberlo. Esta API la hace en una sola llamada, y de ahí
salen las tres cosas que la supervisión necesita:

| Campo | Qué responde |
|---|---|
| `particiones[].lag` | Cuántos mensajes le faltan al grupo en cada partición |
| `lagTotal` | El retraso del grupo entero |
| `distribucion` | Cuánto le toca a cada consumidor, para ver si el reparto quedó parejo |

Tres detalles de la respuesta que conviene conocer:

- **Un `lag` ausente no es cero.** Si el grupo no ha confirmado nada en una partición, el retraso
  no está definido —depende de dónde empiece a leer el consumidor, que lo dice su
  `auto.offset.reset`— así que el campo no se serializa y la partición se cuenta en
  `particionesSinConfirmar` sin sumar al total. Informar un cero diría «está al día», que es lo
  contrario de lo que pasa.
- **`miembro: null` y `particionesSinAsignar`** son la fila que explica un lag que sube sin que
  ningún consumidor esté retrasado: esa partición no la está leyendo nadie.
- **El lag nunca es negativo.** Un offset confirmado por delante del final se ve de verdad durante
  unos milisegundos mientras cambia el líder de una partición, y en un tópico compactado tras una
  limpieza.

### Eliminar un grupo

```bash
curl -s -X DELETE http://localhost:8087/api/v1/kafka/consumer-groups/andesstay-report \
  -H "Authorization: Bearer $TOKEN" -i
```

Se eliminan el grupo y sus offsets; los mensajes siguen en sus tópicos. Un consumidor que vuelva
con el mismo id empezará donde diga su `auto.offset.reset`.

### Reposicionar los offsets de un grupo

```bash
# Al principio del log: el grupo reprocesa todo lo que la retención no se haya llevado
curl -s -X POST http://localhost:8087/api/v1/kafka/consumer-groups/andesstay-audit/offsets/reset \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"estrategia":"earliest"}'
```

```bash
# Al final, saltándose lo pendiente, y solo en un tópico
curl -s -X POST http://localhost:8087/api/v1/kafka/consumer-groups/andesstay-audit/offsets/reset \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"estrategia":"latest","topico":"reservations.events"}'
```

```bash
# A un offset concreto por partición
curl -s -X POST http://localhost:8087/api/v1/kafka/consumer-groups/andesstay-audit/offsets/reset \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"estrategia":"explicito",
       "offsets":[{"topico":"reservations.events","particion":0,"offset":1200},
                  {"topico":"reservations.events","particion":1,"offset":900}]}'
```

| Estrategia | Qué hace | Qué particiones mueve |
|---|---|---|
| `earliest` | Al offset más antiguo que quede en el log | Las que el grupo tiene confirmadas, filtradas por `topico` si llega uno |
| `latest` | Al final | Las mismas |
| `explicito` | A los offsets de la lista | Exactamente las de la lista; no necesita que el grupo las haya consumido antes |

**El grupo tiene que estar sin consumidores conectados.** Con miembros, la respuesta es
`409 GRUPO_ACTIVO`. No es una restricción de esta API: Kafka no permite mover los offsets de un
grupo activo, porque moverlos bajo los pies de un consumidor que está leyendo produciría eventos
duplicados o saltados sin que nadie se entere. Hay que detener los consumidores y esperar a que el
estado pase a `Empty`.

El clúster, por su parte, responde a esto con `UnknownMemberId`, que no le dice nada a quien
llama; el `409` de esta API trae el estado, los miembros conectados y qué hacer.

### Prueba de carga · solo con el perfil `demo`

```bash
curl -s -X POST 'http://localhost:8087/api/v1/kafka/pruebas/carga?count=10000&topico=pruebas.carga' \
  -H "Authorization: Bearer $TOKEN"
```

```json
{"topico":"pruebas.carga","mensajes":10000,"fallidos":0,"duracionMs":2140,
 "mensajesPorSegundo":4672.9}
```

Está para **generar lag visible**. Un grupo sin retraso no demuestra nada: el lag, la distribución
del consumo y el efecto de agregar consumidores solo se ven cuando hay mensajes pendientes de
verdad. Diez mil mensajes sobre un tópico con un consumidor lento bastan para que
`GET /consumer-groups/{id}` y Kafka UI muestren un retraso que baja mientras se mira.

El productor usa los tres valores de la **guía 3.3.2** del curso, más `acks=all`:

| Propiedad | Valor | Por qué |
|---|---|---|
| `batch.size` | 32 KB | Lo que acumula para un mismo broker antes de enviar. Por omisión son 16 KB |
| `linger.ms` | 20 | Lo que espera a que el lote se llene. Por omisión es `0`: sin espera no hay lotes, y sin lotes no hay rendimiento |
| `compression.type` | `snappy` | Comprime menos que gzip y cuesta mucho menos CPU, que es el compromiso que interesa cuando se mide el throughput |
| `acks` | `all` | El clúster tiene `min.insync.replicas=2`; con `acks=1` la prueba mediría un rendimiento que el sistema real no tiene |

La medición espera la confirmación de **todos** los envíos antes de parar el cronómetro. Medir
hasta el `send()` daría un número bonito y falso: `send()` no publica, encola.

**Sin el perfil `demo` la ruta no existe** y responde `404`, no `403`: la clase no se registra. Es
deliberado, porque un administrador que puede inyectar diez mil mensajes en un tópico de
producción no es un administrador.

```bash
./mvnw spring-boot:run -Dspring-boot.run.profiles=demo
```

## La protección de la topología de AndesStay

Los cuatro tópicos del caso están en una lista de **protegidos**
(`andesstay.kafka.protegidos`, en `application.yml`):

```
reservations.events      audit.timeline
reservations.events.DLT  audit.timeline.DLT
```

| Operación | Respuesta |
|---|---|
| `DELETE` de un tópico protegido | `409 RECURSO_PROTEGIDO` |
| `PUT .../config` de un tópico protegido | **Permitido** |
| `POST .../particiones` de un tópico protegido | **Permitido** |

**La protección cubre el borrado y solo el borrado**, y la asimetría es deliberada: eliminar un
tópico en Kafka es irreversible —se va el log de todas sus particiones en los tres brokers y, con
él, los offsets confirmados de los grupos que lo consumían— mientras que un cambio de
`retention.ms` se deshace cambiándolo otra vez. Un `DELETE` accidental sobre `reservations.events`
corta el flujo de eventos hacia `ms-andesstay-audit` y no deja nada que restaurar.

Es además lo que permite demostrar la modificación de configuraciones sobre un tópico real del
caso, en lugar de sobre uno de pruebas.

No hay ningún parámetro que levante la protección. Cambiar la topología del sistema exige cambiar
la lista y volver a desplegar, de modo que no pueda ocurrir por un parámetro de más en una
petición; por lo mismo, la lista **no se expone como variable de entorno**.

El aviso de `409` dice dónde se decide:

```json
{"error":"RECURSO_PROTEGIDO",
 "detalle":"No se permite eliminar el tópico 'reservations.events': forma parte de la topología de AndesStay (andesstay.kafka.protegidos) y esta API no la elimina. Eliminar un tópico en Kafka es irreversible.",
 "nombre":"reservations.events"}
```

## Validación de formato

Los nombres se validan antes de llegar al clúster. Los formatos están en
[`dto/Restricciones.java`](src/main/java/cl/andesstay/kafka/dto/Restricciones.java) y no son
decisiones de esta API, sino límites del propio Kafka:

| Campo | Formato |
|---|---|
| `nombre` de tópico | Letras sin tilde, dígitos y `. _ -`, hasta 249 caracteres |
| `particiones` | De 1 a 200 |
| `factorDeReplicacion` | De 1 a 10 |
| `estrategia` del reset | `earliest`, `latest` o `explicito`, sin distinguir mayúsculas |
| `particion` y `offset` | Enteros desde 0 |

Todos los errores de formato responden igual, con el campo que falla:

```json
{"error":"VALIDACION","detalle":"el cuerpo no es válido",
 "campos":{"nombre":"solo admite letras sin tilde, dígitos y los caracteres . _ -"}}
```

Hay tres nombres que **pasan** el formato y rechaza
[`service/NombresDeTopico.java`](src/main/java/cl/andesstay/kafka/service/NombresDeTopico.java)
con un `400` explicado: `.` y `..`, porque Kafka guarda cada tópico en un directorio con su
nombre, y cualquiera que empiece por `__`, porque es el prefijo de los tópicos internos del
clúster y el listado de esta API lo escondería, dejando el tópico creado e invisible.

## Códigos de error

Todas las respuestas de error tienen la forma `{"error": "<CÓDIGO>", "detalle": "<texto>", …}`,
con campos adicionales según el caso.

| HTTP | `error` | Cuándo |
|---|---|---|
| `400` | `VALIDACION` | Un campo del cuerpo o un parámetro no cumple el formato. Trae `campos` |
| `400` | `CUERPO_ILEGIBLE` | JSON mal formado |
| `400` | `PETICION_INVALIDA` | Nombre reservado, bajada de particiones, estrategia incoherente con los offsets, o un rechazo del clúster que es culpa de la petición |
| `401` | — | Sin token, o token inválido o vencido. Sin cuerpo |
| `403` | `ROL_INSUFICIENTE` | Token válido sin el rol `Admin` |
| `404` | `RECURSO_NO_ENCONTRADO` | El tópico o el grupo no existe. Trae `recurso` y `nombre` |
| `409` | `RECURSO_PROTEGIDO` | `DELETE` sobre la topología de AndesStay. Trae `nombre` |
| `409` | `TOPICO_YA_EXISTE` | Alta de un tópico cuyo nombre ya está tomado. Trae `nombre` |
| `409` | `GRUPO_ACTIVO` | Baja o reset de offsets de un grupo con consumidores. Trae `estado` y `miembros` |
| `502` | `CLUSTER_RECHAZO` | El clúster respondió, pero con un fallo del que no se sabe más |
| `502` | `ERROR_DEL_CLUSTER` | Fallo del productor de la prueba de carga |
| `503` | `CLUSTER_NO_DISPONIBLE` | El clúster no respondió dentro de `andesstay.kafka.timeout-ms` |

La diferencia entre el `502` y el `503` importa para quien integra: un `503` se arregla esperando
a que el clúster vuelva; un `502` no, porque el clúster está.

## Detalles que cuestan una tarde si no se conocen

**`describeConsumerGroups` no falla ante un grupo inexistente.** Devuelve una descripción en
estado `Dead` y sin miembros, idéntica a la de un grupo que existió y se borró, y
`listConsumerGroupOffsets` devuelve un mapa vacío. Sin tratarlo, esta API respondería `200` con un
grupo vacío a cualquier identificador inventado. Lo que distingue a un grupo real en ese estado es
tener offsets confirmados, así que `ConsumerGroupService` cruza las dos señales antes de dar el
grupo por bueno.

**`createTopics` no es idempotente.** A diferencia de `queue.declare` de AMQP, falla con
`TopicExists` aunque las particiones y la configuración coincidan. Un script que se ejecute dos
veces se lo encuentra en la segunda.

**`incrementalAlterConfigs` y `alterConfigs` no son lo mismo.** El segundo **reemplaza** la
configuración completa del recurso: enviar solo `retention.ms` devolvería al valor por omisión
todo lo demás que estuviera puesto. Esta API usa siempre el incremental.

**`UnknownMemberId` significa «el grupo está en marcha».** Es lo que responde Kafka al intentar
mover los offsets de un grupo con miembros, y no lo dice por ningún lado. Aquí sale como un `409`
que lo explica.

**Las particiones solo suben.** Y subirlas tampoco es gratis: el reparto por clave cambia desde
ese momento, así que una misma clave empieza a caer en otra partición y se pierde la garantía de
orden por clave respecto de los mensajes anteriores. La respuesta del `POST` lo dice.

**El `AdminClient` reintenta dos minutos contra un clúster caído.** Es su
`default.api.timeout.ms` por omisión, y durante todo ese rato retiene el hilo de Tomcat que
atiende la petición. De ahí `andesstay.kafka.timeout-ms`, que lo baja a diez segundos y hace que
el fallo salga como un `503` rápido.

## Lo que este servicio NO hace

**No declara la topología de AndesStay.** Los cuatro tópicos del caso los crea el servicio
`kafka-init` de `infra/kafka/compose.yml` al levantar el clúster. Este servicio declara solo lo
que le piden por HTTP.

**No consume ni produce eventos de negocio.** La única publicación es la prueba de carga del
perfil `demo`, y está ahí para generar lag, no para mover datos.

**No reasigna réplicas entre brokers.** `kafka-reassign-partitions` es una operación con estado
—se lanza, se sigue y se verifica— que no encaja en una petición HTTP sin inventar un recurso de
seguimiento. Se hace desde la consola del clúster.

## Stack

Java 17 · Spring Boot 3.2.5 · kafka-clients (`AdminClient`) · Spring for Apache Kafka (solo el
productor del perfil `demo`) · Spring Security (OAuth2 Resource Server) · Actuator.

## Variables de entorno

| Variable | Descripción |
|---|---|
| `KAFKA_BOOTSTRAP` | Lista de brokers, separada por comas. Desde el host, `localhost:9092,localhost:9093,localhost:9094`; dentro de la red de contenedores, `kafka1:29092,kafka2:29093,kafka3:29094` |
| `AZURE_TENANT_ID` | Tenant de Entra ID |
| `AZURE_CLIENT_ID` | Registro de aplicación que emite el token |

Se configuran en un archivo `.env` que **no se versiona**. Ver [`.env.example`](.env.example).

La lista de protegidos **no es una variable de entorno** a propósito: desactivarla por
configuración sería perder justo lo que aporta.

## Salud

`/actuator/health` es lo único abierto sin token, porque lo consulta el healthcheck del
contenedor. Comprueba el clúster con `describeCluster` y queda `DOWN` si no responde o si no tiene
controlador elegido —un clúster sin controlador responde, pero no acepta crear ni borrar
tópicos—. Un administrador que no alcanza su clúster no sirve para nada, así que el contenedor
debe quedar unhealthy; es el mismo criterio que `ms-andesstay-mq`.

`/actuator/metrics` **no** está expuesto: describiría el clúster a cualquiera que alcance el
puerto.

## Cómo levantarlo

Requiere el clúster de Kafka en marcha. El compose está en el repositorio
[`infra`](https://github.com/AndesStay-Duoc/infra):

```bash
docker compose -f infra/kafka/compose.yml up -d
```

Levanta tres Zookeeper, tres brokers, el `kafka-init` que crea los tópicos del caso y Kafka UI en
[http://localhost:8090](http://localhost:8090).

```bash
cp .env.example .env
./mvnw spring-boot:run
```

Con la prueba de carga disponible:

```bash
./mvnw spring-boot:run -Dspring-boot.run.profiles=demo
```

En contenedor:

```bash
docker build -t ms-andesstay-kafka .
docker run --rm -p 8087:8087 --network andesstay-kafka_andesstay-kafka \
  -e KAFKA_BOOTSTRAP=kafka1:29092,kafka2:29093,kafka3:29094 ms-andesstay-kafka
```

## Pruebas

```bash
./mvnw verify
```

Cubren, sin clúster ni tokens reales: la traducción de los errores del `AdminClient` a las
excepciones propias, el cálculo del lag partición por partición y su distribución por consumidor,
la protección de la topología, el rechazo de la bajada de particiones, la existencia de un grupo
cuando Kafka responde `Dead`, la matriz de códigos HTTP de los dos recursos, y que la prueba de
carga no exista sin el perfil `demo`.

## Contratos

Los tópicos y los esquemas de evento son canónicos y viven en
[`infra/docs/contracts/events/kafka.md`](https://github.com/AndesStay-Duoc/infra/blob/develop/docs/contracts/events/kafka.md).

## Cómo contribuir

Ver [`CONTRIBUTING.md`](CONTRIBUTING.md).
