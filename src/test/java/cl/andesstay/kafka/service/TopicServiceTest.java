package cl.andesstay.kafka.service;

import cl.andesstay.kafka.broker.KafkaAdminClient;
import cl.andesstay.kafka.broker.KafkaAdminClient.ConfiguracionApi;
import cl.andesstay.kafka.broker.KafkaAdminClient.NodoApi;
import cl.andesstay.kafka.broker.KafkaAdminClient.ParticionDescripcionApi;
import cl.andesstay.kafka.broker.KafkaAdminClient.TopicoApi;
import cl.andesstay.kafka.config.KafkaProperties;
import cl.andesstay.kafka.dto.AumentarParticionesRequest;
import cl.andesstay.kafka.dto.CrearTopicoRequest;
import cl.andesstay.kafka.dto.ModificarConfiguracionRequest;
import cl.andesstay.kafka.dto.OperacionResultado;
import cl.andesstay.kafka.dto.TopicoDetalle;
import cl.andesstay.kafka.dto.TopicoResumen;
import cl.andesstay.kafka.web.RecursoNoEncontradoException;
import cl.andesstay.kafka.web.RecursoProtegidoException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Lo que de verdad puede salir mal en el servicio de tópicos.
 *
 * No se prueba que el AdminClient hable bien con el cluster —eso es de kafka-clients y está
 * cubierto en KafkaAdminClientTest— sino las decisiones propias: qué nombres se rechazan antes de
 * molestar al cluster, qué se esconde del listado, qué operaciones protege la topología y por qué
 * las particiones solo suben.
 *
 * Las tres que importan más son el seguro de los protegidos, la asimetría entre eliminar y
 * configurar, y el rechazo de la bajada de particiones: las tres son sitios donde un cambio
 * inocente produce una respuesta mentirosa o un destrozo irreversible.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TopicServiceTest {

    private static final String PROTEGIDO = "reservations.events";
    private static final String LIBRE = "pruebas.demo";

    @Mock
    private KafkaAdminClient cluster;

    private TopicService servicio;

    @BeforeEach
    void preparar() {
        KafkaProperties propiedades = new KafkaProperties(
                "localhost:9092", 10_000,
                List.of(PROTEGIDO, "audit.timeline"),
                new KafkaProperties.Pruebas("pruebas.carga", 10_000));
        servicio = new TopicService(cluster, propiedades);
    }

    private static CrearTopicoRequest peticion(String nombre) {
        return new CrearTopicoRequest(nombre, null, null, null);
    }

    /** Un tópico sano: tantas particiones como se pida, con tres réplicas todas al día. */
    private static TopicoApi sano(String nombre, int particiones) {
        return reparto(nombre, particiones, 3, 3, true);
    }

    private static TopicoApi reparto(String nombre, int particiones, int replicas, int isr,
                                     boolean conLider) {
        List<ParticionDescripcionApi> detalle = new ArrayList<>();
        for (int i = 0; i < particiones; i++) {
            detalle.add(new ParticionDescripcionApi(i,
                    conLider ? nodo(1) : null, nodos(replicas), nodos(isr)));
        }
        return new TopicoApi(nombre, false, detalle);
    }

    private static List<NodoApi> nodos(int cuantos) {
        List<NodoApi> nodos = new ArrayList<>();
        for (int i = 1; i <= cuantos; i++) {
            nodos.add(nodo(i));
        }
        return nodos;
    }

    private static NodoApi nodo(int id) {
        return new NodoApi(id, "kafka" + id, 29090 + id);
    }

    private void existe(TopicoApi topico) {
        when(cluster.describirTopico(topico.nombre())).thenReturn(Optional.of(topico));
    }

    // ── Alta ─────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("crear un tópico sin particiones ni factor usa los del cluster del caso: 3 y 3")
    void valoresPorOmision() {
        servicio.crear(peticion(LIBRE));

        // Es el motivo de que los dos campos del DTO sean Integer y no int: con un primitivo,
        // omitirlos habría pedido cero particiones y el cluster habría rechazado la creación.
        verify(cluster).crearTopico(LIBRE, 3, (short) 3, Map.of());
    }

    @Test
    @DisplayName("las configuraciones llegan al cluster tal cual, sin filtrar")
    void configuracionesSinFiltrar() {
        Map<String, String> configuraciones = Map.of(
                "retention.ms", "604800000",
                "cleanup.policy", "compact,delete");

        servicio.crear(new CrearTopicoRequest(LIBRE, 6, 3, configuraciones));

        // Replicar aquí la tabla de configuraciones de Kafka sería una copia condenada a quedarse
        // atrás: valida el cluster, y lo que esta API garantiza es no estorbar.
        verify(cluster).crearTopico(LIBRE, 6, (short) 3, configuraciones);
    }

    @Test
    @DisplayName("la respuesta del alta dice qué quedó creado, no solo que no hubo error")
    void respuestaDelAltaExplica() {
        OperacionResultado resultado = servicio.crear(new CrearTopicoRequest(LIBRE, 6, 2, null));

        assertEquals("creado", resultado.operacion());
        assertTrue(resultado.detalle().contains("6 partición(es)"), resultado.detalle());
        assertTrue(resultado.detalle().contains("hereda las del broker"), resultado.detalle());
    }

    @Nested
    @DisplayName("los nombres que no se admiten")
    class NombresRechazados {

        @ParameterizedTest
        @ValueSource(strings = {"__consumer_offsets", "__propio", "__"})
        @DisplayName("400 · el prefijo __ está reservado por Kafka, y el listado lo escondería")
        void prefijoInterno(String nombre) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> servicio.crear(peticion(nombre)));

            assertTrue(e.getMessage().contains("__"), e.getMessage());
            // Lo esencial: no se llegó a tocar el cluster.
            verify(cluster, never()).crearTopico(anyString(), anyInt(), anyShort(), anyMap());
        }

        @ParameterizedTest
        @ValueSource(strings = {".", ".."})
        @DisplayName("400 · '.' y '..' ya significan algo en un sistema de archivos")
        void puntos(String nombre) {
            assertThrows(IllegalArgumentException.class, () -> servicio.crear(peticion(nombre)));
            verify(cluster, never()).crearTopico(anyString(), anyInt(), anyShort(), anyMap());
        }

        @Test
        @DisplayName("un nombre que solo CONTIENE __ sí se acepta")
        void contenerNoEsEmpezar() {
            // La restricción es sobre el prefijo. Rechazar por contener dejaría fuera nombres
            // legítimos.
            servicio.crear(peticion("pruebas__demo"));

            verify(cluster).crearTopico(eq("pruebas__demo"), anyInt(), anyShort(), anyMap());
        }
    }

    // ── Listado ──────────────────────────────────────────────────────────────

    @Test
    @DisplayName("el listado esconde los internos del cluster y los de Kafka Streams")
    void listadoEscondeInternos() {
        when(cluster.nombresDeTopicos()).thenReturn(new java.util.TreeSet<>(Set.of(
                LIBRE, "__consumer_offsets", "_schemas",
                "app-estado-changelog", "app-agrupado-repartition")));
        when(cluster.describirTopicos(List.of(LIBRE)))
                .thenReturn(Map.of(LIBRE, sano(LIBRE, 3)));

        List<TopicoResumen> topicos = servicio.listar(false);

        assertEquals(1, topicos.size());
        assertEquals(LIBRE, topicos.get(0).nombre());
        // Se piden al cluster solo los que van a salir: describir __consumer_offsets, con sus
        // cincuenta particiones, es trabajo tirado.
        verify(cluster).describirTopicos(List.of(LIBRE));
    }

    @Test
    @DisplayName("con incluirInternos=true aparecen también los escondidos")
    void listadoConInternos() {
        when(cluster.nombresDeTopicos())
                .thenReturn(new java.util.TreeSet<>(Set.of(LIBRE, "__consumer_offsets")));
        when(cluster.describirTopicos(any())).thenReturn(Map.of(
                LIBRE, sano(LIBRE, 3),
                "__consumer_offsets", new TopicoApi("__consumer_offsets", true,
                        sano("__consumer_offsets", 2).particiones())));

        List<TopicoResumen> topicos = servicio.listar(true);

        assertEquals(2, topicos.size());
        assertEquals("__consumer_offsets", topicos.get(0).nombre(), "ordenado por nombre");
        assertTrue(topicos.get(0).interno());
    }

    @Test
    @DisplayName("el listado marca los tópicos protegidos y resume el reparto")
    void listadoMarcaProtegidos() {
        when(cluster.nombresDeTopicos())
                .thenReturn(new java.util.TreeSet<>(Set.of(PROTEGIDO, LIBRE)));
        when(cluster.describirTopicos(any())).thenReturn(Map.of(
                PROTEGIDO, sano(PROTEGIDO, 3),
                LIBRE, sano(LIBRE, 1)));

        List<TopicoResumen> topicos = servicio.listar(false);

        TopicoResumen protegido = topicos.stream()
                .filter(t -> t.nombre().equals(PROTEGIDO)).findFirst().orElseThrow();
        assertTrue(protegido.protegido());
        assertEquals(3, protegido.particiones());
        assertEquals(3, protegido.factorDeReplicacion());
        assertEquals(3, protegido.replicasEnSincronia());
        assertFalse(protegido.degradado());
    }

    @Test
    @DisplayName("un tópico con menos réplicas al día que réplicas sale como degradado")
    void degradadoSeDetecta() {
        when(cluster.nombresDeTopicos()).thenReturn(new java.util.TreeSet<>(Set.of(LIBRE)));
        // Tres réplicas y solo dos al día: con min.insync.replicas=2 está al borde de dejar de
        // aceptar publicaciones con acks=all.
        when(cluster.describirTopicos(any()))
                .thenReturn(Map.of(LIBRE, reparto(LIBRE, 3, 3, 2, true)));

        TopicoResumen resumen = servicio.listar(false).get(0);

        assertTrue(resumen.degradado());
        assertEquals(2, resumen.replicasEnSincronia());
    }

    @Test
    @DisplayName("una partición sin líder también deja el tópico degradado")
    void sinLiderEsDegradado() {
        when(cluster.nombresDeTopicos()).thenReturn(new java.util.TreeSet<>(Set.of(LIBRE)));
        when(cluster.describirTopicos(any()))
                .thenReturn(Map.of(LIBRE, reparto(LIBRE, 1, 3, 3, false)));

        // Sin líder no se publica ni se consume en esa partición, aunque las réplicas estén.
        assertTrue(servicio.listar(false).get(0).degradado());
    }

    // ── Detalle ──────────────────────────────────────────────────────────────

    @Test
    @DisplayName("el detalle separa la configuración propia del tópico de la heredada")
    void detalleSeparaLaConfiguracion() {
        existe(sano(LIBRE, 2));
        when(cluster.configuracionDeTopico(LIBRE)).thenReturn(List.of(
                new ConfiguracionApi("retention.ms", "60000", "DYNAMIC_TOPIC_CONFIG",
                        false, false, false),
                new ConfiguracionApi("cleanup.policy", "delete", "DEFAULT_CONFIG",
                        true, false, true)));

        TopicoDetalle detalle = servicio.describir(LIBRE);

        // Todas las claves efectivas, que es lo que responde «y entonces cuánto retiene».
        assertEquals(2, detalle.configuraciones().size());
        // Y solo las propias, que es lo que responde «qué distingue a este tópico».
        assertEquals(Map.of("retention.ms", "60000"), detalle.configuracionesPersonalizadas());
        assertEquals(2, detalle.detalleDeParticiones().size());
        assertTrue(detalle.detalleDeParticiones().get(0).enSincronia());
    }

    @Test
    @DisplayName("404 · describir un tópico inexistente")
    void describirInexistenteEs404() {
        when(cluster.describirTopico(LIBRE)).thenReturn(Optional.empty());

        RecursoNoEncontradoException e = assertThrows(RecursoNoEncontradoException.class,
                () -> servicio.describir(LIBRE));

        assertEquals("tópico", e.getRecurso());
        assertEquals(LIBRE, e.getNombre());
    }

    // ── Configuración ────────────────────────────────────────────────────────

    @Test
    @DisplayName("un tópico PROTEGIDO sí admite cambios de configuración: son reversibles")
    void protegidoSeConfigura() {
        existe(sano(PROTEGIDO, 3));

        OperacionResultado resultado = servicio.modificarConfiguracion(PROTEGIDO,
                new ModificarConfiguracionRequest(Map.of("retention.ms", "604800000")));

        // Es la asimetría deliberada del servicio: la protección cubre lo irreversible, y cambiar
        // retention.ms no lo es. Es además lo que la evaluación pide demostrar sobre un tópico
        // real del caso.
        verify(cluster).alterarConfiguraciones(PROTEGIDO, Map.of("retention.ms", "604800000"));
        assertEquals("configurado", resultado.operacion());
        assertTrue(resultado.detalle().contains("el resto de la configuración quedó como estaba"),
                resultado.detalle());
    }

    @Test
    @DisplayName("404 · configurar un tópico inexistente no llega al cluster")
    void configurarInexistenteEs404() {
        when(cluster.describirTopico(LIBRE)).thenReturn(Optional.empty());

        assertThrows(RecursoNoEncontradoException.class, () -> servicio.modificarConfiguracion(
                LIBRE, new ModificarConfiguracionRequest(Map.of("retention.ms", "1"))));

        verify(cluster, never()).alterarConfiguraciones(anyString(), anyMap());
    }

    // ── Particiones ──────────────────────────────────────────────────────────

    @Test
    @DisplayName("aumentar particiones pasa el total y avisa de que el reparto por clave cambia")
    void aumentarParticiones() {
        existe(sano(LIBRE, 3));

        OperacionResultado resultado = servicio.aumentarParticiones(LIBRE,
                new AumentarParticionesRequest(6));

        verify(cluster).crearParticiones(LIBRE, 6);
        assertEquals("reparticionado", resultado.operacion());
        // El aviso importa: desde el aumento, una misma clave puede caer en otra partición y se
        // pierde el orden por clave respecto de lo ya publicado.
        assertTrue(resultado.detalle().contains("orden por clave"), resultado.detalle());
    }

    @ParameterizedTest(name = "pedir {0} teniendo 3")
    @ValueSource(ints = {1, 2, 3})
    @DisplayName("400 · Kafka no permite reducir las particiones, y el mensaje lo explica")
    void reducirParticionesEs400(int pedidas) {
        existe(sano(LIBRE, 3));

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> servicio.aumentarParticiones(LIBRE, new AumentarParticionesRequest(pedidas)));

        assertTrue(e.getMessage().contains("no permite reducir"), e.getMessage());
        assertTrue(e.getMessage().contains("crear otro tópico y migrar"), e.getMessage());
        verify(cluster, never()).crearParticiones(anyString(), anyInt());
    }

    @Test
    @DisplayName("404 · aumentar las particiones de un tópico inexistente")
    void aumentarInexistenteEs404() {
        when(cluster.describirTopico(LIBRE)).thenReturn(Optional.empty());

        assertThrows(RecursoNoEncontradoException.class,
                () -> servicio.aumentarParticiones(LIBRE, new AumentarParticionesRequest(6)));
    }

    // ── Baja, con la protección de la topología ──────────────────────────────

    @Test
    @DisplayName("un tópico de la topología de AndesStay no se elimina")
    void protegidoNoSeElimina() {
        RecursoProtegidoException e = assertThrows(RecursoProtegidoException.class,
                () -> servicio.eliminar(PROTEGIDO));

        assertEquals(PROTEGIDO, e.getNombre());
        assertTrue(e.getMessage().contains("andesstay.kafka.protegidos"), e.getMessage());
        // Lo esencial: no se llegó a borrar nada, y ni siquiera se consultó. Eliminar un tópico en
        // Kafka se lleva el log de todas sus particiones y no tiene vuelta atrás.
        verify(cluster, never()).eliminarTopico(anyString());
        verify(cluster, never()).describirTopico(PROTEGIDO);
    }

    @Test
    @DisplayName("404 · eliminar un tópico inexistente, sin pedírselo al cluster")
    void eliminarInexistenteEs404() {
        when(cluster.describirTopico(LIBRE)).thenReturn(Optional.empty());

        assertThrows(RecursoNoEncontradoException.class, () -> servicio.eliminar(LIBRE));

        verify(cluster, never()).eliminarTopico(anyString());
    }

    @Test
    @DisplayName("un tópico libre se elimina, y la respuesta dice que fue irreversible")
    void libreSeElimina() {
        existe(sano(LIBRE, 3));

        OperacionResultado resultado = servicio.eliminar(LIBRE);

        verify(cluster).eliminarTopico(LIBRE);
        assertEquals("eliminado", resultado.operacion());
        assertTrue(resultado.detalle().contains("irreversible"), resultado.detalle());
    }
}
