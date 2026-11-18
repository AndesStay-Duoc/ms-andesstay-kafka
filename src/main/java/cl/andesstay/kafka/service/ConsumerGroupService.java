package cl.andesstay.kafka.service;

import cl.andesstay.kafka.broker.KafkaAdminClient;
import cl.andesstay.kafka.broker.KafkaAdminClient.ParticionApi;
import cl.andesstay.kafka.dto.ConsumoPorMiembro;
import cl.andesstay.kafka.dto.GrupoDetalle;
import cl.andesstay.kafka.dto.GrupoResumen;
import cl.andesstay.kafka.dto.MiembroResumen;
import cl.andesstay.kafka.dto.NodoResumen;
import cl.andesstay.kafka.dto.OffsetDeParticion;
import cl.andesstay.kafka.dto.OperacionResultado;
import cl.andesstay.kafka.dto.ParticionDeGrupo;
import cl.andesstay.kafka.dto.ResetOffsetsRequest;
import cl.andesstay.kafka.web.GrupoActivoException;
import cl.andesstay.kafka.web.RecursoNoEncontradoException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Grupos de consumidores: quién consume, por dónde va, cuánto le falta y cómo se reposiciona.
 *
 * EL LAG NO LO CALCULA KAFKA. No hay ninguna operación del AdminClient que lo devuelva: lo que
 * hay son dos lecturas separadas, el offset que el grupo confirmó (listConsumerGroupOffsets) y el
 * offset final de cada partición (listOffsets), y la resta la hace quien quiera saberlo. Esta
 * clase la hace una vez y bien, que es justo lo que la supervisión necesita y lo que la consola
 * de Kafka no da sin un comando por grupo.
 *
 * EL CASO QUE HAY QUE CONOCER: describeConsumerGroups NO falla ante un grupo inexistente. El
 * cluster devuelve una descripción en estado Dead, sin miembros, idéntica a la de un grupo que
 * existió y se borró, y listConsumerGroupOffsets devuelve un mapa vacío. Si esto no se tratara,
 * la API respondería 200 con un grupo vacío ante cualquier identificador inventado. De ahí
 * exigirQueExista, que cruza las dos señales antes de dar por bueno el grupo.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ConsumerGroupService {

    private static final String RECURSO = "grupo";

    /**
     * El estado con el que el cluster responde por un grupo que no conoce.
     *
     * Se compara sin distinguir mayúsculas porque el valor viene del toString() del enum de Kafka
     * y no conviene que un cambio de formato de su parte rompa la comprobación en silencio.
     */
    private static final String ESTADO_INEXISTENTE = "Dead";

    private final KafkaAdminClient cluster;

    /** Lista los grupos que el cluster conoce, con su estado. */
    public List<GrupoResumen> listar() {
        return cluster.grupos().stream()
                .map(grupo -> new GrupoResumen(grupo.id(), grupo.estado(), grupo.simple()))
                .toList();
    }

    /**
     * El detalle del grupo con su lag, partición por partición y por consumidor.
     *
     * Son tres llamadas al cluster y una resta:
     *
     *   describeConsumerGroups    quiénes están conectados y qué particiones tiene cada uno
     *   listConsumerGroupOffsets  por dónde va el grupo en cada partición
     *   listOffsets               dónde termina hoy cada partición
     *
     * Las particiones que se miran son la unión de las que el grupo tiene confirmadas y las que
     * sus miembros tienen asignadas. No son lo mismo: una partición recién asignada todavía no
     * tiene offset confirmado, y una que el grupo consumió hace un mes sigue teniendo offset
     * aunque ahora no la lea nadie. Las dos interesan, y la segunda es justamente la que explica
     * un lag que no baja.
     */
    public GrupoDetalle describir(String id) {
        KafkaAdminClient.GrupoDescripcionApi descripcion = exigirQueExista(id);
        Map<ParticionApi, Long> confirmados = cluster.offsetsConfirmados(id);

        // Qué miembro tiene asignada cada partición. Se guarda el client.id y no el consumerId
        // porque el segundo cambia en cada reconexión y no identifica al servicio.
        Map<ParticionApi, String> asignacion = new LinkedHashMap<>();
        Map<String, String> hostDelMiembro = new LinkedHashMap<>();
        for (KafkaAdminClient.MiembroApi miembro : descripcion.miembros()) {
            miembro.asignacion().forEach(particion -> asignacion.put(particion, miembro.cliente()));
            hostDelMiembro.put(miembro.cliente(), miembro.host());
        }

        Set<ParticionApi> particiones = new LinkedHashSet<>(confirmados.keySet());
        particiones.addAll(asignacion.keySet());
        Map<ParticionApi, Long> finales = cluster.offsetsFinales(particiones);

        List<ParticionDeGrupo> filas = new ArrayList<>(particiones.size());
        long lagTotal = 0;
        int sinConfirmar = 0;
        int sinAsignar = 0;

        for (ParticionApi particion : particiones.stream().sorted().toList()) {
            Long confirmado = confirmados.get(particion);
            Long ultimo = finales.get(particion);
            String miembro = asignacion.get(particion);
            Long lag = lag(confirmado, ultimo);

            if (confirmado == null) {
                sinConfirmar++;
            }
            if (miembro == null) {
                sinAsignar++;
            }
            if (lag != null) {
                lagTotal += lag;
            }
            filas.add(new ParticionDeGrupo(particion.topico(), particion.particion(),
                    confirmado, ultimo, lag, miembro));
        }

        return new GrupoDetalle(
                descripcion.id(),
                descripcion.estado(),
                descripcion.simple(),
                descripcion.coordinador() == null ? null : new NodoResumen(
                        descripcion.coordinador().id(),
                        descripcion.coordinador().host(),
                        descripcion.coordinador().puerto()),
                descripcion.estrategiaDeAsignacion(),
                descripcion.miembros().stream().map(ConsumerGroupService::aMiembro).toList(),
                filas,
                distribucion(filas, hostDelMiembro),
                lagTotal,
                sinConfirmar,
                sinAsignar);
    }

    /**
     * Elimina el grupo y sus offsets confirmados.
     *
     * Exige que no tenga miembros conectados, porque Kafka no lo permite: el grupo es estado
     * compartido y borrarlo bajo los pies de un consumidor que está leyendo lo dejaría sin saber
     * por dónde iba. Con miembros, la respuesta es 409 y no el GroupNotEmpty del cluster, que no
     * le dice nada a quien llama.
     *
     * Lo que se pierde al borrar son los offsets: los mensajes siguen en sus tópicos. Un grupo con
     * el mismo id que vuelva a conectarse empieza donde diga su auto.offset.reset.
     */
    public OperacionResultado eliminar(String id) {
        KafkaAdminClient.GrupoDescripcionApi descripcion = exigirQueExista(id);
        exigirQueNoTengaMiembros(descripcion, "eliminar");

        cluster.eliminarGrupo(id);
        return OperacionResultado.de(RECURSO, id, "eliminado",
                "se eliminaron el grupo y sus offsets confirmados; los mensajes siguen en sus "
                        + "tópicos, y un consumidor que vuelva con este id empezará donde diga su "
                        + "auto.offset.reset");
    }

    /**
     * Reposiciona los offsets del grupo.
     *
     * Las tres estrategias están descritas en ResetOffsetsRequest. Lo que decide esta clase es
     * QUÉ particiones se mueven con earliest y latest: las que el grupo tiene confirmadas,
     * limitadas al tópico indicado si llega uno. No se inventan particiones que el grupo nunca
     * consumió; para eso está la estrategia explicito.
     *
     * El grupo tiene que estar sin miembros conectados, por el mismo motivo que la baja.
     */
    public OperacionResultado reposicionar(String id, ResetOffsetsRequest peticion) {
        validarCoherencia(peticion);

        KafkaAdminClient.GrupoDescripcionApi descripcion = exigirQueExista(id);
        exigirQueNoTengaMiembros(descripcion, "mover los offsets de");

        Map<ParticionApi, Long> destino = peticion.esExplicita()
                ? destinoExplicito(peticion.offsetsOVacios())
                : destinoPorExtremo(id, peticion);

        cluster.alterarOffsets(id, destino);
        log.warn("[Grupos] Reposicionado {} con la estrategia {} sobre {} partición(es)",
                id, peticion.estrategiaNormalizada(), destino.size());

        return OperacionResultado.de(RECURSO, id, "reposicionado",
                "se movieron " + destino.size() + " partición(es) con la estrategia "
                        + peticion.estrategiaNormalizada() + ": " + descripcionDeDestino(destino));
    }

    // ── Reglas de existencia y de estado ─────────────────────────────────────

    /**
     * Devuelve la descripción del grupo, o 404 si el cluster no lo conoce.
     *
     * No basta con que describeConsumerGroups devuelva algo: ante un identificador inventado
     * responde con estado Dead y sin miembros, exactamente igual que ante un grupo borrado. Lo
     * que distingue a un grupo real en ese estado es tener offsets confirmados, así que la
     * segunda lectura solo se hace cuando hace falta, que es cuando el estado es Dead.
     */
    private KafkaAdminClient.GrupoDescripcionApi exigirQueExista(String id) {
        KafkaAdminClient.GrupoDescripcionApi descripcion = cluster.describirGrupo(id)
                .orElseThrow(() -> new RecursoNoEncontradoException(RECURSO, id));

        boolean pareceInexistente = ESTADO_INEXISTENTE.equalsIgnoreCase(descripcion.estado())
                && descripcion.miembros().isEmpty();

        if (pareceInexistente && cluster.offsetsConfirmados(id).isEmpty()) {
            throw new RecursoNoEncontradoException(RECURSO, id);
        }
        return descripcion;
    }

    private void exigirQueNoTengaMiembros(KafkaAdminClient.GrupoDescripcionApi descripcion,
                                          String operacion) {
        if (!descripcion.miembros().isEmpty()) {
            List<String> miembros = descripcion.miembros().stream()
                    .map(KafkaAdminClient.MiembroApi::cliente)
                    .toList();
            throw new GrupoActivoException(descripcion.id(), descripcion.estado(), miembros,
                    operacion);
        }
    }

    /**
     * La coherencia entre la estrategia y los offsets, que no se puede expresar con anotaciones
     * porque es una regla entre dos campos.
     */
    private static void validarCoherencia(ResetOffsetsRequest peticion) {
        if (peticion.esExplicita()) {
            if (peticion.offsetsOVacios().isEmpty()) {
                throw new IllegalArgumentException(
                        "la estrategia explicito necesita la lista de offsets, con un tópico, una "
                                + "partición y un offset por entrada");
            }
            if (peticion.topico() != null) {
                throw new IllegalArgumentException(
                        "el campo topico no se usa con la estrategia explicito: cada entrada de "
                                + "offsets ya lleva el suyo");
            }
            return;
        }
        if (!peticion.offsetsOVacios().isEmpty()) {
            throw new IllegalArgumentException(
                    "la lista de offsets solo se usa con la estrategia explicito; con "
                            + peticion.estrategiaNormalizada()
                            + " los offsets los decide el cluster");
        }
    }

    // ── Construcción del destino ─────────────────────────────────────────────

    private Map<ParticionApi, Long> destinoExplicito(List<OffsetDeParticion> offsets) {
        Map<ParticionApi, Long> destino = new LinkedHashMap<>();
        Map<String, Integer> particionesPorTopico = new LinkedHashMap<>();

        for (OffsetDeParticion offset : offsets) {
            // El tópico se comprueba una sola vez aunque venga en varias entradas: sin esto, una
            // partición inexistente sale como un error genérico del cluster en lugar de un 404.
            int particiones = particionesPorTopico.computeIfAbsent(offset.topico(),
                    nombre -> cluster.describirTopico(nombre)
                            .orElseThrow(() -> new RecursoNoEncontradoException("tópico", nombre))
                            .particiones().size());

            if (offset.particion() >= particiones) {
                throw new IllegalArgumentException("el tópico '" + offset.topico() + "' tiene "
                        + particiones + " partición(es), numeradas de 0 a " + (particiones - 1)
                        + "; no existe la " + offset.particion());
            }
            destino.put(new ParticionApi(offset.topico(), offset.particion()), offset.offset());
        }
        return destino;
    }

    private Map<ParticionApi, Long> destinoPorExtremo(String id, ResetOffsetsRequest peticion) {
        List<ParticionApi> particiones = cluster.offsetsConfirmados(id).keySet().stream()
                .filter(particion -> peticion.topico() == null
                        || peticion.topico().equals(particion.topico()))
                .sorted()
                .toList();

        if (particiones.isEmpty()) {
            throw new IllegalArgumentException(peticion.topico() == null
                    ? "el grupo '" + id + "' no tiene ningún offset confirmado, así que no hay "
                            + "nada que mover. Para posicionarlo en particiones que nunca consumió, "
                            + "usa la estrategia explicito"
                    : "el grupo '" + id + "' no tiene offsets confirmados en el tópico '"
                            + peticion.topico() + "'");
        }

        return ResetOffsetsRequest.EARLIEST.equals(peticion.estrategiaNormalizada())
                ? cluster.offsetsIniciales(particiones)
                : cluster.offsetsFinales(particiones);
    }

    // ── Apoyo ────────────────────────────────────────────────────────────────

    /**
     * El lag de una partición: cuántos mensajes quedan entre donde va el grupo y el final.
     *
     * Null cuando no hay offset confirmado, porque entonces el retraso no está definido: depende
     * de dónde decida empezar el consumidor cuando se conecte, y eso lo dice su auto.offset.reset,
     * no el cluster.
     *
     * Nunca negativo. Un confirmado por delante del final se ve de verdad durante unos
     * milisegundos mientras el líder de la partición cambia, y en un tópico compactado tras una
     * limpieza; informar «-3 mensajes pendientes» solo confundiría.
     */
    static Long lag(Long confirmado, Long ultimo) {
        if (confirmado == null || ultimo == null) {
            return null;
        }
        return Math.max(0L, ultimo - confirmado);
    }

    private static MiembroResumen aMiembro(KafkaAdminClient.MiembroApi miembro) {
        return new MiembroResumen(
                miembro.id(),
                miembro.cliente(),
                miembro.host(),
                miembro.asignacion().stream()
                        // «tópico-partición», que es como lo escriben Kafka y su consola.
                        .map(particion -> particion.topico() + "-" + particion.particion())
                        .toList());
    }

    /**
     * El mismo lag agrupado por consumidor, que es la distribución del consumo.
     *
     * Las particiones que no tiene asignadas nadie se juntan en una fila con miembro null. Es la
     * fila que explica un lag que sube sin que ningún consumidor esté retrasado: no hay quien las
     * lea.
     */
    private static List<ConsumoPorMiembro> distribucion(List<ParticionDeGrupo> filas,
                                                        Map<String, String> hostDelMiembro) {
        // TreeMap con nulls al final: el reparto se lee por consumidor y las particiones
        // huérfanas son la excepción que conviene ver al final, no mezclada.
        Map<String, int[]> particionesPorMiembro = new TreeMap<>(
                Comparator.nullsLast(Comparator.naturalOrder()));
        Map<String, long[]> lagPorMiembro = new TreeMap<>(
                Comparator.nullsLast(Comparator.naturalOrder()));

        for (ParticionDeGrupo fila : filas) {
            particionesPorMiembro.computeIfAbsent(fila.miembro(), m -> new int[1])[0]++;
            if (fila.lag() != null) {
                lagPorMiembro.computeIfAbsent(fila.miembro(), m -> new long[1])[0] += fila.lag();
            }
        }

        List<ConsumoPorMiembro> distribucion = new ArrayList<>(particionesPorMiembro.size());
        particionesPorMiembro.forEach((miembro, particiones) -> distribucion.add(
                new ConsumoPorMiembro(
                        miembro,
                        hostDelMiembro.get(miembro),
                        particiones[0],
                        lagPorMiembro.containsKey(miembro) ? lagPorMiembro.get(miembro)[0] : 0L)));
        return distribucion;
    }

    /** Un resumen legible del destino, para el campo detalle de la respuesta. */
    private static String descripcionDeDestino(Map<ParticionApi, Long> destino) {
        return destino.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entrada -> entrada.getKey().topico() + "-" + entrada.getKey().particion()
                        + " a " + entrada.getValue())
                .reduce((a, b) -> a + ", " + b)
                .orElse("ninguna");
    }
}
