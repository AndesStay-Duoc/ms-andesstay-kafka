package cl.andesstay.kafka.service;

import cl.andesstay.kafka.broker.KafkaAdminClient;
import cl.andesstay.kafka.config.KafkaProperties;
import cl.andesstay.kafka.dto.AumentarParticionesRequest;
import cl.andesstay.kafka.dto.CrearTopicoRequest;
import cl.andesstay.kafka.dto.ModificarConfiguracionRequest;
import cl.andesstay.kafka.dto.NodoResumen;
import cl.andesstay.kafka.dto.OperacionResultado;
import cl.andesstay.kafka.dto.ParticionDetalle;
import cl.andesstay.kafka.dto.TopicoDetalle;
import cl.andesstay.kafka.dto.TopicoResumen;
import cl.andesstay.kafka.web.RecursoNoEncontradoException;
import cl.andesstay.kafka.web.RecursoProtegidoException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Altas, bajas, cambios de configuración y consultas de tópicos.
 *
 * Aquí vive la lógica de negocio del recurso: qué se puede crear, qué hay que comprobar antes de
 * destruir y cómo se traduce lo que informa el cluster. La conversación con el AdminClient está
 * en broker/KafkaAdminClient, y esta clase solo lo recibe ya construido.
 *
 * Las dos reglas que no son evidentes al leer la API:
 *
 *   un tópico protegido no se elimina nunca, ni con parámetro de por medio, pero SÍ se le cambia
 *   la configuración: lo primero es irreversible y lo segundo no;
 *
 *   las particiones solo suben. No es una decisión de esta API: Kafka no implementa la bajada, y
 *   el motivo es que los mensajes ya están repartidos por clave entre las particiones que hay.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TopicService {

    private static final String RECURSO = "tópico";

    private final KafkaAdminClient cluster;
    private final KafkaProperties propiedades;

    /**
     * Crea el tópico.
     *
     * A diferencia de la declaración de una cola en AMQP, esto NO es idempotente: si el nombre ya
     * está tomado, el cluster responde TopicExists aunque las particiones y la configuración
     * coincidan, y la API devuelve 409. Es una diferencia real con ms-andesstay-mq y conviene
     * tenerla presente al escribir un script que se ejecute dos veces.
     */
    public OperacionResultado crear(CrearTopicoRequest peticion) {
        NombresDeTopico.validar(peticion.nombre());

        int particiones = peticion.particionesOPorOmision();
        short replicacion = peticion.factorOPorOmision();
        Map<String, String> configuraciones = peticion.configuracionesOVacias();

        cluster.crearTopico(peticion.nombre(), particiones, replicacion, configuraciones);

        String detalle = "creado con " + particiones + " partición(es) y factor de replicación "
                + replicacion
                + (configuraciones.isEmpty()
                        ? "; sin configuraciones propias, hereda las del broker"
                        : "; configuraciones " + configuraciones.keySet());
        return OperacionResultado.de(RECURSO, peticion.nombre(), "creado", detalle);
    }

    /**
     * Lista los tópicos con su reparto entre los brokers.
     *
     * Son dos llamadas al cluster y no una: listTopics da los nombres y describeTopics el reparto.
     * No hay forma de pedir las dos cosas juntas.
     *
     * @param incluirInternos true para ver también __consumer_offsets y los tópicos de trabajo de
     *                        Kafka Streams, que normalmente solo son ruido. Ver TopicosInternos
     */
    public List<TopicoResumen> listar(boolean incluirInternos) {
        List<String> nombres = cluster.nombresDeTopicos().stream()
                .filter(nombre -> incluirInternos || !TopicosInternos.esInterno(nombre))
                .toList();

        return cluster.describirTopicos(nombres).values().stream()
                .filter(topico -> incluirInternos || !topico.interno())
                .map(this::aResumen)
                .sorted(Comparator.comparing(TopicoResumen::nombre))
                .toList();
    }

    /** Describe un tópico: su reparto partición por partición y su configuración efectiva. */
    public TopicoDetalle describir(String nombre) {
        KafkaAdminClient.TopicoApi topico = cluster.describirTopico(nombre)
                .orElseThrow(() -> new RecursoNoEncontradoException(RECURSO, nombre));

        Map<String, String> todas = new LinkedHashMap<>();
        Map<String, String> propias = new LinkedHashMap<>();
        for (KafkaAdminClient.ConfiguracionApi configuracion : cluster.configuracionDeTopico(nombre)) {
            todas.put(configuracion.nombre(), configuracion.valor());
            if (!configuracion.porOmision()) {
                propias.put(configuracion.nombre(), configuracion.valor());
            }
        }

        List<ParticionDetalle> particiones = topico.particiones().stream()
                .map(TopicService::aParticionDetalle)
                .sorted(Comparator.comparingInt(ParticionDetalle::particion))
                .toList();

        return new TopicoDetalle(
                topico.nombre(),
                topico.interno(),
                propiedades.esProtegido(topico.nombre()),
                particiones.size(),
                factorDeReplicacion(topico),
                estaDegradado(topico),
                particiones,
                todas,
                propias);
    }

    /**
     * Modifica configuraciones del tópico sin tocar las que no llegan en la petición.
     *
     * Se permite también sobre los tópicos protegidos, y es deliberado: un cambio de retention.ms
     * o de cleanup.policy es reversible, mientras que un borrado no. La protección cubre lo que no
     * tiene vuelta atrás.
     *
     * Se comprueba antes que el tópico exista porque incrementalAlterConfigs sobre un nombre
     * inexistente responde con un error genérico del cluster, y un 404 es mucho más claro.
     */
    public OperacionResultado modificarConfiguracion(String nombre,
                                                     ModificarConfiguracionRequest peticion) {
        exigirQueExista(nombre);
        cluster.alterarConfiguraciones(nombre, peticion.configuraciones());

        if (propiedades.esProtegido(nombre)) {
            log.warn("[Tópicos] Se modificó la configuración de {}, que está protegido: {}",
                    nombre, peticion.configuraciones().keySet());
        }
        return OperacionResultado.de(RECURSO, nombre, "configurado",
                "se modificaron las claves " + peticion.configuraciones().keySet()
                        + "; el resto de la configuración quedó como estaba");
    }

    /**
     * Sube el número de particiones del tópico.
     *
     * KAFKA NO PERMITE BAJARLO, y por eso la petición se rechaza con un 400 que lo explica en
     * lugar de dejar que el cluster responda InvalidPartitions. El motivo de fondo es que los
     * mensajes ya publicados están repartidos por el resto de la clave entre las particiones que
     * había, y quitar una dejaría sus mensajes sin ningún sitio donde estar.
     *
     * Subirlas tampoco es gratis, aunque el cluster lo acepte: el reparto por clave cambia desde
     * ese momento, así que una misma clave empieza a caer en otra partición y se pierde la
     * garantía de orden por clave para los mensajes anteriores. Queda dicho en la respuesta.
     */
    public OperacionResultado aumentarParticiones(String nombre,
                                                  AumentarParticionesRequest peticion) {
        KafkaAdminClient.TopicoApi topico = cluster.describirTopico(nombre)
                .orElseThrow(() -> new RecursoNoEncontradoException(RECURSO, nombre));

        int actuales = topico.particiones().size();
        int total = peticion.particiones();

        if (total <= actuales) {
            throw new IllegalArgumentException(
                    "el tópico '" + nombre + "' ya tiene " + actuales + " particiones y el total "
                            + "pedido es " + total + ". Kafka no permite reducir las particiones "
                            + "de un tópico: los mensajes ya publicados están repartidos entre las "
                            + "que hay. Para bajarlas hay que crear otro tópico y migrar.");
        }

        cluster.crearParticiones(nombre, total);
        log.warn("[Tópicos] {} pasó de {} a {} particiones; el reparto por clave cambia desde ahora",
                nombre, actuales, total);

        return OperacionResultado.de(RECURSO, nombre, "reparticionado",
                "pasó de " + actuales + " a " + total + " particiones. El reparto por clave cambia "
                        + "desde ahora: una misma clave puede caer en otra partición, así que el "
                        + "orden por clave no se mantiene respecto de los mensajes anteriores");
    }

    /**
     * Elimina el tópico.
     *
     * Es irreversible y se lleva el log de todas sus particiones en los tres brokers, junto con
     * los offsets que los grupos tuvieran confirmados sobre él. De ahí la protección de los
     * nombres de la topología de AndesStay, que no se levanta con ningún parámetro.
     *
     * La protección se comprueba ANTES que la existencia, igual que en ms-andesstay-mq: un 409
     * sobre un tópico protegido no debería depender de si el nombre existe o no.
     */
    public OperacionResultado eliminar(String nombre) {
        if (propiedades.esProtegido(nombre)) {
            throw new RecursoProtegidoException(RECURSO, nombre, "eliminar");
        }
        // Se comprueba antes de borrar porque deleteTopics sobre un nombre inexistente responde
        // con UnknownTopicOrPartition, y un 404 explicado es mejor respuesta que la traducción
        // automática de ese error. Es el mismo criterio que toma ms-andesstay-mq con getQueueInfo.
        exigirQueExista(nombre);

        cluster.eliminarTopico(nombre);
        return OperacionResultado.de(RECURSO, nombre, "eliminado",
                "el tópico y el log de todas sus particiones se eliminaron de forma irreversible");
    }

    // ── Apoyo ────────────────────────────────────────────────────────────────

    private void exigirQueExista(String nombre) {
        if (cluster.describirTopico(nombre).isEmpty()) {
            throw new RecursoNoEncontradoException(RECURSO, nombre);
        }
    }

    private TopicoResumen aResumen(KafkaAdminClient.TopicoApi topico) {
        return new TopicoResumen(
                topico.nombre(),
                topico.interno(),
                propiedades.esProtegido(topico.nombre()),
                topico.particiones().size(),
                factorDeReplicacion(topico),
                replicasEnSincronia(topico),
                estaDegradado(topico));
    }

    /**
     * El factor de replicación del tópico es el número de réplicas de sus particiones.
     *
     * Se toma el MÁXIMO entre las particiones porque, durante una reasignación, dos particiones
     * del mismo tópico pueden tener distinto número de réplicas por un rato. El máximo es el
     * factor hacia el que el tópico va.
     */
    private static int factorDeReplicacion(KafkaAdminClient.TopicoApi topico) {
        return topico.particiones().stream()
                .mapToInt(particion -> particion.replicas().size())
                .max()
                .orElse(0);
    }

    /**
     * El mínimo de réplicas al día entre las particiones del tópico.
     *
     * Es el número que importa para saber si el tópico sigue aceptando publicaciones: con
     * min.insync.replicas en 2, que es lo que tiene el cluster del caso, basta con que UNA
     * partición baje de dos réplicas al día para que las publicaciones con acks=all a esa
     * partición empiecen a fallar.
     */
    private static int replicasEnSincronia(KafkaAdminClient.TopicoApi topico) {
        return topico.particiones().stream()
                .mapToInt(particion -> particion.isr().size())
                .min()
                .orElse(0);
    }

    private static boolean estaDegradado(KafkaAdminClient.TopicoApi topico) {
        return topico.particiones().stream()
                .anyMatch(particion -> particion.lider() == null
                        || particion.isr().size() < particion.replicas().size());
    }

    private static ParticionDetalle aParticionDetalle(
            KafkaAdminClient.ParticionDescripcionApi particion) {
        List<NodoResumen> replicas = aNodos(particion.replicas());
        List<NodoResumen> isr = aNodos(particion.isr());
        return new ParticionDetalle(
                particion.particion(),
                particion.lider() == null ? null : aNodo(particion.lider()),
                replicas,
                isr,
                particion.lider() != null && isr.size() == replicas.size());
    }

    private static List<NodoResumen> aNodos(List<KafkaAdminClient.NodoApi> nodos) {
        List<NodoResumen> resumen = new ArrayList<>(nodos.size());
        nodos.forEach(nodo -> resumen.add(aNodo(nodo)));
        return resumen;
    }

    private static NodoResumen aNodo(KafkaAdminClient.NodoApi nodo) {
        return new NodoResumen(nodo.id(), nodo.host(), nodo.puerto());
    }
}
