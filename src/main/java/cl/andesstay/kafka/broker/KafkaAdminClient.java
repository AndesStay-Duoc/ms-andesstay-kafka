package cl.andesstay.kafka.broker;

import cl.andesstay.kafka.config.KafkaProperties;
import cl.andesstay.kafka.web.ApiDeGestionException;
import cl.andesstay.kafka.web.GrupoActivoException;
import cl.andesstay.kafka.web.RecursoNoEncontradoException;
import cl.andesstay.kafka.web.RecursoYaExisteException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.GroupSubscribedToTopicException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Todo lo que este servicio sabe hacer contra el cluster, y el único sitio que conoce la API de
 * Kafka.
 *
 * POR QUÉ EXISTE ESTA CLASE. Es el mismo papel que cumple broker/ManagementApiClient en
 * ms-andesstay-mq: aislar el cliente del broker detrás de un vocabulario propio. Aquí la razón es
 * todavía más fuerte, porque el AdminClient tiene tres asperezas que no conviene repartir por los
 * servicios:
 *
 *   1. Devuelve futuros. Cada operación es un KafkaFuture que hay que esperar con un límite, y
 *      olvidarlo en un solo sitio significa un hilo de Tomcat retenido dos minutos.
 *   2. Los errores llegan envueltos en ExecutionException y hay que mirar la causa para saber si
 *      lo que pasó fue «no existe», «ya existe» o «el cluster no está». Esa traducción está aquí,
 *      una vez, y de ella salen las excepciones propias del paquete web.
 *   3. Habla en TopicPartition, Node y ConfigEntry. Los servicios reciben records de este paquete.
 *
 * Se inyecta la interfaz Admin y no la clase AdminClient por el mismo motivo por el que
 * ms-andesstay-mq inyecta AmqpAdmin: es lo que permite sustituirla por un doble en las pruebas sin
 * levantar ningún cluster.
 */
@Slf4j
@Component
public class KafkaAdminClient {

    private final Admin admin;
    private final Duration limite;

    public KafkaAdminClient(Admin admin, KafkaProperties propiedades) {
        this.admin = admin;
        this.limite = propiedades.timeout();
    }

    // ── Tópicos: escrituras ──────────────────────────────────────────────────

    /**
     * Crea el tópico.
     *
     * Al contrario que `queue.declare` de AMQP, createTopics NO es idempotente: si el nombre ya
     * está tomado, el cluster responde TopicExists aunque las particiones y la configuración
     * coincidan exactamente. Por eso sale como 409 y no como un alta repetida inofensiva.
     */
    public void crearTopico(String nombre, int particiones, short replicacion,
                            Map<String, String> configuraciones) {
        NewTopic topico = new NewTopic(nombre, particiones, replicacion);
        if (configuraciones != null && !configuraciones.isEmpty()) {
            topico.configs(configuraciones);
        }
        esperar(admin.createTopics(List.of(topico)).all(), "crear el tópico", "tópico", nombre);
        log.info("[Tópicos] Creado {} con {} partición(es), factor {} y configuraciones {}",
                nombre, particiones, replicacion, configuraciones);
    }

    /** Elimina el tópico, con el log de todas sus particiones. Es irreversible. */
    public void eliminarTopico(String nombre) {
        esperar(admin.deleteTopics(List.of(nombre)).all(), "eliminar el tópico", "tópico", nombre);
        log.warn("[Tópicos] Eliminado {} y el log de todas sus particiones", nombre);
    }

    /**
     * Modifica configuraciones del tópico sin tocar las demás.
     *
     * Usa incrementalAlterConfigs y no el alterConfigs clásico, y la diferencia importa: el
     * segundo REEMPLAZA la configuración completa del recurso, así que enviar solo retention.ms
     * devolvería al valor por omisión todo lo demás que estuviera puesto. El incremental aplica
     * operación por operación.
     *
     * Un valor null en el mapa se traduce a la operación DELETE, que devuelve esa clave a su valor
     * por omisión; es la única forma de QUITAR una configuración en lugar de fijarla.
     */
    public void alterarConfiguraciones(String topico, Map<String, String> cambios) {
        List<AlterConfigOp> operaciones = cambios.entrySet().stream()
                .map(cambio -> new AlterConfigOp(
                        new ConfigEntry(cambio.getKey(), cambio.getValue()),
                        cambio.getValue() == null
                                ? AlterConfigOp.OpType.DELETE
                                : AlterConfigOp.OpType.SET))
                .toList();

        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, topico);
        esperar(admin.incrementalAlterConfigs(Map.of(recurso, operaciones)).all(),
                "modificar la configuración del tópico", "tópico", topico);
        log.info("[Tópicos] Modificada la configuración de {}: {}", topico, cambios);
    }

    /**
     * Sube el número de particiones hasta el total indicado.
     *
     * El parámetro es el TOTAL que tendrá el tópico, no cuántas se agregan: es como lo define
     * createPartitions, y cambiar el significado aquí solo invitaría a equivocarse al leer el log
     * del cluster. Kafka no permite bajarlo; eso lo explica TopicService antes de llegar aquí.
     */
    public void crearParticiones(String topico, int total) {
        esperar(admin.createPartitions(Map.of(topico, NewPartitions.increaseTo(total))).all(),
                "aumentar las particiones del tópico", "tópico", topico);
        log.info("[Tópicos] {} pasó a tener {} particiones", topico, total);
    }

    // ── Tópicos: lecturas ────────────────────────────────────────────────────

    /**
     * Los nombres de todos los tópicos, internos incluidos.
     *
     * Se piden siempre con listInternal(true) y el filtrado lo decide TopicService. El motivo es
     * que la opción del cliente solo conoce los internos declarados como tales por el cluster
     * (__consumer_offsets), y no los changelog ni los repartition de Kafka Streams, que son
     * tópicos normales con nombre convenido. Filtrar en un solo sitio evita que la respuesta
     * dependa de cuál de los dos criterios se aplicó.
     */
    public Set<String> nombresDeTopicos() {
        Set<String> nombres = esperar(
                admin.listTopics(new ListTopicsOptions().listInternal(true)).names(),
                "listar los tópicos", null, null);
        return new TreeSet<>(nombres);
    }

    /**
     * Describe varios tópicos de una vez.
     *
     * Se usan los futuros por tópico (topicNameValues) en lugar del futuro agregado (allTopicNames)
     * para que un tópico eliminado ENTRE el listado y la descripción no tire la respuesta entera:
     * se omite el que ya no está y se devuelven los demás. En un listado que se consulta mientras
     * alguien administra, esa carrera ocurre.
     */
    public Map<String, TopicoApi> describirTopicos(Collection<String> nombres) {
        if (nombres.isEmpty()) {
            return Map.of();
        }
        Map<String, KafkaFuture<TopicDescription>> futuros =
                admin.describeTopics(nombres).topicNameValues();

        Map<String, TopicoApi> descripciones = new LinkedHashMap<>();
        futuros.forEach((nombre, futuro) -> {
            try {
                descripciones.put(nombre, aTopicoApi(esperar(futuro, "describir el tópico",
                        "tópico", nombre)));
            } catch (RecursoNoEncontradoException e) {
                log.debug("[Tópicos] {} desapareció entre el listado y la descripción", nombre);
            }
        });
        return descripciones;
    }

    /** Un solo tópico, o vacío si no existe. */
    public Optional<TopicoApi> describirTopico(String nombre) {
        try {
            return Optional.of(aTopicoApi(esperar(
                    admin.describeTopics(List.of(nombre)).topicNameValues().get(nombre),
                    "describir el tópico", "tópico", nombre)));
        } catch (RecursoNoEncontradoException e) {
            return Optional.empty();
        }
    }

    /**
     * La configuración efectiva del tópico: lo que el cluster aplica de verdad, no solo lo que
     * alguien fijó.
     *
     * Trae del orden de sesenta claves, la mayoría heredadas del broker. De ahí que cada entrada
     * diga de dónde sale su valor: es lo que permite distinguir un retention.ms puesto a mano de
     * uno que viene del server.properties del cluster.
     */
    public List<ConfiguracionApi> configuracionDeTopico(String nombre) {
        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
        Config configuracion = esperar(admin.describeConfigs(List.of(recurso)).all(),
                "leer la configuración del tópico", "tópico", nombre).get(recurso);

        if (configuracion == null) {
            return List.of();
        }
        return configuracion.entries().stream()
                .map(entrada -> new ConfiguracionApi(
                        entrada.name(),
                        entrada.value(),
                        entrada.source().name(),
                        // DYNAMIC_TOPIC_CONFIG es el valor fijado sobre el tópico; cualquier otro
                        // origen es herencia del broker o el valor por omisión de Kafka.
                        entrada.source() != ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG,
                        entrada.isSensitive(),
                        entrada.isReadOnly()))
                .sorted((a, b) -> a.nombre().compareTo(b.nombre()))
                .toList();
    }

    // ── Grupos de consumidores ───────────────────────────────────────────────

    /**
     * Los grupos que el cluster conoce, con su estado.
     *
     * Se usa valid() y no all(): con un broker de los tres caído, all() falla entero y valid()
     * devuelve los grupos que sí se pudieron listar. Para una API de diagnóstico, media respuesta
     * es mejor que ninguna justo cuando algo va mal.
     */
    public List<GrupoApi> grupos() {
        Collection<ConsumerGroupListing> listados =
                esperar(admin.listConsumerGroups().valid(), "listar los grupos", null, null);

        return listados.stream()
                .map(listado -> new GrupoApi(
                        listado.groupId(),
                        // toString() y no name(): el enum se llama PREPARING_REBALANCE pero se
                        // escribe PreparingRebalance en la consola de Kafka y en Kafka UI, y una
                        // API que los nombra de otra forma obliga a traducir mentalmente.
                        listado.state().map(Object::toString).orElse("Unknown"),
                        listado.isSimpleConsumerGroup()))
                .sorted((a, b) -> a.id().compareTo(b.id()))
                .toList();
    }

    /**
     * Describe un grupo, o vacío si el cluster no lo conoce.
     *
     * CUIDADO CON ESTA OPERACIÓN. describeConsumerGroups NO falla ante un grupo inexistente: el
     * cluster devuelve una descripción con estado Dead y sin miembros, igual que la de un grupo
     * que existió y se borró. Devolver eso como un 200 sería mentir, así que quien decide el 404
     * es ConsumerGroupService, cruzando el estado con los offsets confirmados. Aquí solo se
     * traduce el GroupIdNotFound que algunos brokers sí lanzan.
     */
    public Optional<GrupoDescripcionApi> describirGrupo(String id) {
        try {
            ConsumerGroupDescription descripcion = esperar(
                    admin.describeConsumerGroups(List.of(id)).all(),
                    "describir el grupo", "grupo", id).get(id);
            return Optional.ofNullable(descripcion).map(KafkaAdminClient::aGrupoApi);

        } catch (RecursoNoEncontradoException e) {
            return Optional.empty();
        }
    }

    /**
     * Los offsets confirmados del grupo, partición por partición.
     *
     * Un grupo inexistente devuelve un mapa vacío en lugar de fallar, que es la otra mitad de la
     * tolerancia descrita en describirGrupo.
     */
    public Map<ParticionApi, Long> offsetsConfirmados(String grupo) {
        var confirmados = esperar(
                admin.listConsumerGroupOffsets(grupo).partitionsToOffsetAndMetadata(),
                "leer los offsets confirmados del grupo", "grupo", grupo);

        Map<ParticionApi, Long> offsets = new LinkedHashMap<>();
        confirmados.forEach((particion, metadatos) -> {
            // El valor es null cuando el grupo tiene la partición asignada pero todavía no ha
            // confirmado nada en ella.
            if (metadatos != null) {
                offsets.put(aParticionApi(particion), metadatos.offset());
            }
        });
        return offsets;
    }

    /** El offset final (el que tendrá el próximo mensaje publicado) de cada partición. */
    public Map<ParticionApi, Long> offsetsFinales(Collection<ParticionApi> particiones) {
        return offsets(particiones, OffsetSpec.latest(), "leer los offsets finales");
    }

    /** El offset más antiguo que queda en el log de cada partición, tras la retención. */
    public Map<ParticionApi, Long> offsetsIniciales(Collection<ParticionApi> particiones) {
        return offsets(particiones, OffsetSpec.earliest(), "leer los offsets iniciales");
    }

    /** Elimina el grupo y sus offsets confirmados. Exige que no tenga miembros conectados. */
    public void eliminarGrupo(String id) {
        esperar(admin.deleteConsumerGroups(List.of(id)).all(), "eliminar", "grupo", id);
        log.warn("[Grupos] Eliminado {} con sus offsets confirmados", id);
    }

    /**
     * Mueve los offsets confirmados del grupo.
     *
     * Solo funciona con el grupo sin miembros conectados. Con miembros, el cluster responde
     * UnknownMemberId, que aquí se traduce a GrupoActivoException porque ese nombre no le dice
     * nada a quien llama.
     */
    public void alterarOffsets(String grupo, Map<ParticionApi, Long> nuevos) {
        Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> destino =
                new LinkedHashMap<>();
        nuevos.forEach((particion, offset) -> destino.put(
                new TopicPartition(particion.topico(), particion.particion()),
                new org.apache.kafka.clients.consumer.OffsetAndMetadata(offset)));

        esperar(admin.alterConsumerGroupOffsets(grupo, destino).all(),
                "mover los offsets", "grupo", grupo);
        log.warn("[Grupos] Movidos los offsets de {}: {}", grupo, nuevos);
    }

    // ── Cluster ──────────────────────────────────────────────────────────────

    /** Identificador y nodos del cluster. Lo usa el indicador de salud. */
    public ClusterApi describirCluster() {
        var resultado = admin.describeCluster();
        String id = esperar(resultado.clusterId(), "leer el identificador del cluster", null, null);
        Collection<Node> nodos = esperar(resultado.nodes(), "listar los brokers", null, null);
        Node controlador = esperar(resultado.controller(), "leer el controlador", null, null);

        return new ClusterApi(id,
                nodos.stream().map(KafkaAdminClient::aNodoApi).toList(),
                controlador == null ? null : aNodoApi(controlador));
    }

    // ── Apoyo ────────────────────────────────────────────────────────────────

    private Map<ParticionApi, Long> offsets(Collection<ParticionApi> particiones,
                                            OffsetSpec cual, String operacion) {
        if (particiones.isEmpty()) {
            return Map.of();
        }
        Map<TopicPartition, OffsetSpec> consulta = particiones.stream()
                .collect(Collectors.toMap(
                        p -> new TopicPartition(p.topico(), p.particion()),
                        p -> cual,
                        (a, b) -> a,
                        LinkedHashMap::new));

        var resultado = esperar(admin.listOffsets(consulta).all(), operacion, null, null);

        Map<ParticionApi, Long> offsets = new LinkedHashMap<>();
        resultado.forEach((particion, info) -> offsets.put(aParticionApi(particion), info.offset()));
        return offsets;
    }

    /**
     * Espera el futuro con el límite configurado y traduce lo que haya salido mal.
     *
     * Es el corazón de la clase. Todo lo que el AdminClient puede devolver como fallo pasa por
     * aquí y sale convertido en una excepción del paquete web, de modo que ni los servicios ni el
     * manejador de errores tengan que conocer un solo tipo de org.apache.kafka.
     *
     * @param recurso y nombre identifican el objeto de la operación, para poder responder 404 con
     *                sentido. Van en null en las operaciones que no tratan sobre uno concreto,
     *                como el listado, y entonces un «no existe» del cluster sale como 502: es un
     *                fallo inesperado y no una respuesta normal.
     */
    private <T> T esperar(KafkaFuture<T> futuro, String operacion, String recurso, String nombre) {
        try {
            return futuro.get(limite.toMillis(), TimeUnit.MILLISECONDS);

        } catch (InterruptedException e) {
            // Se restaura la marca de interrupción: tragársela deja al hilo sin saber que le
            // pidieron parar.
            Thread.currentThread().interrupt();
            throw new ApiDeGestionException(
                    "se interrumpió la espera al " + operacion, false, e);

        } catch (java.util.concurrent.TimeoutException e) {
            log.error("[Cluster] Venció el límite de {} ms al {}", limite.toMillis(), operacion);
            throw new ApiDeGestionException(
                    "venció el límite al " + operacion, false, e);

        } catch (ExecutionException e) {
            throw traducir(e.getCause() == null ? e : e.getCause(), operacion, recurso, nombre);
        }
    }

    /**
     * Convierte el fallo del cluster en la excepción propia que le corresponde.
     *
     * El orden de las ramas es el de lo que ocurre a diario administrando: no existe, ya existe,
     * el grupo está en marcha, la petición no es válida, el cluster no está.
     */
    private RuntimeException traducir(Throwable causa, String operacion,
                                      String recurso, String nombre) {
        if (causa instanceof UnknownTopicOrPartitionException
                || causa instanceof GroupIdNotFoundException) {
            if (recurso != null) {
                return new RecursoNoEncontradoException(recurso, nombre);
            }
            return new ApiDeGestionException("el cluster no encontró el recurso al " + operacion,
                    true, causa);
        }
        if (causa instanceof TopicExistsException) {
            return new RecursoYaExisteException(nombre, causa.getMessage());
        }
        if (causa instanceof GroupNotEmptyException
                || causa instanceof UnknownMemberIdException
                || causa instanceof GroupSubscribedToTopicException) {
            // El estado y los miembros concretos los conoce el servicio, que es quien describe el
            // grupo antes de operar; esta rama cubre la carrera de un consumidor que se conecta
            // justo entre la comprobación y la operación.
            return new GrupoActivoException(nombre, "con miembros conectados", List.of(), operacion);
        }
        if (causa instanceof InvalidPartitionsException
                || causa instanceof InvalidReplicationFactorException
                || causa instanceof InvalidConfigurationException
                || causa instanceof InvalidTopicException
                || causa instanceof InvalidRequestException
                || causa instanceof PolicyViolationException
                || causa instanceof UnsupportedVersionException) {
            // El mensaje del cluster nombra el parámetro concreto que no cuadra, que es más útil
            // que cualquier texto propio.
            return new IllegalArgumentException(
                    "el cluster rechazó la petición al " + operacion + ": " + causa.getMessage());
        }
        if (causa instanceof org.apache.kafka.common.errors.TimeoutException) {
            log.error("[Cluster] Sin respuesta al {}: {}", operacion, causa.getMessage());
            return new ApiDeGestionException("no hubo respuesta del cluster al " + operacion,
                    false, causa);
        }
        log.error("[Cluster] Fallo inesperado al {}", operacion, causa);
        return new ApiDeGestionException(
                "el cluster falló al " + operacion + ": " + causa.getMessage(), true, causa);
    }

    private static TopicoApi aTopicoApi(TopicDescription descripcion) {
        List<ParticionDescripcionApi> particiones = new ArrayList<>();
        for (TopicPartitionInfo info : descripcion.partitions()) {
            particiones.add(new ParticionDescripcionApi(
                    info.partition(),
                    info.leader() == null ? null : aNodoApi(info.leader()),
                    info.replicas().stream().map(KafkaAdminClient::aNodoApi).toList(),
                    info.isr().stream().map(KafkaAdminClient::aNodoApi).toList()));
        }
        return new TopicoApi(descripcion.name(), descripcion.isInternal(), particiones);
    }

    private static GrupoDescripcionApi aGrupoApi(ConsumerGroupDescription descripcion) {
        List<MiembroApi> miembros = new ArrayList<>();
        for (MemberDescription miembro : descripcion.members()) {
            miembros.add(new MiembroApi(
                    miembro.consumerId(),
                    miembro.clientId(),
                    miembro.host(),
                    miembro.assignment().topicPartitions().stream()
                            .map(KafkaAdminClient::aParticionApi)
                            .sorted()
                            .toList()));
        }
        return new GrupoDescripcionApi(
                descripcion.groupId(),
                descripcion.state().toString(),
                descripcion.isSimpleConsumerGroup(),
                descripcion.coordinator() == null ? null : aNodoApi(descripcion.coordinator()),
                descripcion.partitionAssignor(),
                miembros);
    }

    private static NodoApi aNodoApi(Node nodo) {
        return new NodoApi(nodo.id(), nodo.host(), nodo.port());
    }

    private static ParticionApi aParticionApi(TopicPartition particion) {
        return new ParticionApi(particion.topic(), particion.partition());
    }

    // ── Proyecciones de lo que informa el cluster ────────────────────────────
    // Son los tipos con los que hablan los servicios. Existen para que TopicPartition, Node y
    // ConfigEntry no salgan de este paquete.

    /** Un broker del cluster. */
    public record NodoApi(int id, String host, int puerto) {
    }

    /** Una partición de un tópico. Ordena por tópico y luego por número, como el cluster. */
    public record ParticionApi(String topico, int particion) implements Comparable<ParticionApi> {

        @Override
        public int compareTo(ParticionApi otra) {
            int porTopico = topico.compareTo(otra.topico);
            return porTopico != 0 ? porTopico : Integer.compare(particion, otra.particion);
        }
    }

    /**
     * El reparto de una partición entre los brokers.
     *
     * @param lider    el broker que atiende las lecturas y escrituras; null mientras no hay
     *                 ninguno elegido, que es lo que se ve con una partición sin servicio
     * @param replicas los brokers que guardan una copia
     * @param isr      las réplicas al día. Que isr sea menor que replicas es la señal de que el
     *                 tópico está degradado, y con min.insync.replicas en 2 es lo que hace que las
     *                 publicaciones empiecen a fallar
     */
    public record ParticionDescripcionApi(int particion, NodoApi lider,
                                          List<NodoApi> replicas, List<NodoApi> isr) {
    }

    public record TopicoApi(String nombre, boolean interno,
                            List<ParticionDescripcionApi> particiones) {
    }

    /**
     * Una clave de configuración y de dónde sale su valor.
     *
     * @param origen      el ConfigSource del cluster, tal cual: DYNAMIC_TOPIC_CONFIG,
     *                    DEFAULT_CONFIG, STATIC_BROKER_CONFIG...
     * @param porOmision  true si el valor no está fijado sobre el tópico, sino heredado
     */
    public record ConfiguracionApi(String nombre, String valor, String origen,
                                   boolean porOmision, boolean sensible, boolean soloLectura) {
    }

    public record GrupoApi(String id, String estado, boolean simple) {
    }

    /** Un consumidor conectado al grupo y las particiones que tiene asignadas. */
    public record MiembroApi(String id, String cliente, String host, List<ParticionApi> asignacion) {
    }

    public record GrupoDescripcionApi(String id, String estado, boolean simple,
                                      NodoApi coordinador, String estrategiaDeAsignacion,
                                      List<MiembroApi> miembros) {
    }

    public record ClusterApi(String id, List<NodoApi> nodos, NodoApi controlador) {
    }
}
