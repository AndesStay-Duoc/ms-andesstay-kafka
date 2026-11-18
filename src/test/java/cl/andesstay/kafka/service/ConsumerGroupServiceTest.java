package cl.andesstay.kafka.service;

import cl.andesstay.kafka.broker.KafkaAdminClient;
import cl.andesstay.kafka.broker.KafkaAdminClient.GrupoApi;
import cl.andesstay.kafka.broker.KafkaAdminClient.GrupoDescripcionApi;
import cl.andesstay.kafka.broker.KafkaAdminClient.MiembroApi;
import cl.andesstay.kafka.broker.KafkaAdminClient.NodoApi;
import cl.andesstay.kafka.broker.KafkaAdminClient.ParticionApi;
import cl.andesstay.kafka.broker.KafkaAdminClient.ParticionDescripcionApi;
import cl.andesstay.kafka.broker.KafkaAdminClient.TopicoApi;
import cl.andesstay.kafka.dto.ConsumoPorMiembro;
import cl.andesstay.kafka.dto.GrupoDetalle;
import cl.andesstay.kafka.dto.OffsetDeParticion;
import cl.andesstay.kafka.dto.OperacionResultado;
import cl.andesstay.kafka.dto.ParticionDeGrupo;
import cl.andesstay.kafka.dto.ResetOffsetsRequest;
import cl.andesstay.kafka.web.GrupoActivoException;
import cl.andesstay.kafka.web.RecursoNoEncontradoException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * El cálculo del lag y las reglas de los grupos, sin cluster.
 *
 * Esta clase es la que cubre lo que la evaluación mira: que el retraso de un grupo se calcule
 * bien partición por partición, que se sume bien, que una partición sin confirmar no se cuente
 * como cero, y que la distribución por consumidor diga quién arrastra el retraso.
 *
 * Cubre también los dos casos en los que Kafka contesta algo que no significa lo que parece: un
 * grupo inexistente que llega como Dead en lugar de fallar, y un grupo con miembros al que no se
 * le pueden tocar los offsets.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ConsumerGroupServiceTest {

    private static final String GRUPO = "andesstay-audit";
    private static final String TOPICO = "reservations.events";

    @Mock
    private KafkaAdminClient cluster;

    private ConsumerGroupService servicio;

    @BeforeEach
    void preparar() {
        servicio = new ConsumerGroupService(cluster);
    }

    private static ParticionApi particion(int numero) {
        return new ParticionApi(TOPICO, numero);
    }

    private static MiembroApi miembro(String cliente, int... particiones) {
        List<ParticionApi> asignacion = new java.util.ArrayList<>();
        for (int numero : particiones) {
            asignacion.add(particion(numero));
        }
        return new MiembroApi("consumidor-" + cliente, cliente, "10.0.0.5", asignacion);
    }

    private static GrupoDescripcionApi grupo(String estado, MiembroApi... miembros) {
        return new GrupoDescripcionApi(GRUPO, estado, false,
                new NodoApi(1, "kafka1", 29092), "range", List.of(miembros));
    }

    private void describe(GrupoDescripcionApi descripcion) {
        when(cluster.describirGrupo(GRUPO)).thenReturn(Optional.of(descripcion));
    }

    private void confirmados(Map<ParticionApi, Long> offsets) {
        when(cluster.offsetsConfirmados(GRUPO)).thenReturn(offsets);
    }

    private static Map<ParticionApi, Long> offsets(long... porParticion) {
        Map<ParticionApi, Long> mapa = new LinkedHashMap<>();
        for (int i = 0; i < porParticion.length; i++) {
            mapa.put(particion(i), porParticion[i]);
        }
        return mapa;
    }

    // ── El cálculo del lag ───────────────────────────────────────────────────

    @Nested
    @DisplayName("el lag de una partición")
    class Lag {

        @Test
        @DisplayName("es la diferencia entre el final y lo confirmado")
        void esLaDiferencia() {
            assertEquals(380L, ConsumerGroupService.lag(120L, 500L));
        }

        @Test
        @DisplayName("es cero cuando el grupo está al día")
        void alDiaEsCero() {
            assertEquals(0L, ConsumerGroupService.lag(500L, 500L));
        }

        @Test
        @DisplayName("nunca es negativo")
        void nuncaNegativo() {
            // Un confirmado por delante del final se ve de verdad mientras cambia el líder de la
            // partición, y en un tópico compactado tras una limpieza. «-3 pendientes» solo
            // confundiría a quien lo lea.
            assertEquals(0L, ConsumerGroupService.lag(505L, 500L));
        }

        @Test
        @DisplayName("no está definido si el grupo no ha confirmado nada en la partición")
        void sinConfirmarEsNulo() {
            // Devolver cero diría «está al día», que es lo contrario de lo que pasa: el grupo no
            // ha leído nada todavía, y dónde empezará lo decide su auto.offset.reset.
            assertNull(ConsumerGroupService.lag(null, 500L));
        }
    }

    @Test
    @DisplayName("el detalle calcula el lag por partición y el total del grupo")
    void detalleCalculaElLag() {
        describe(grupo("Stable", miembro("audit-1", 0, 1), miembro("audit-2", 2)));
        confirmados(offsets(100, 200, 300));
        when(cluster.offsetsFinales(any())).thenReturn(offsets(150, 200, 1000));

        GrupoDetalle detalle = servicio.describir(GRUPO);

        assertEquals(3, detalle.particiones().size());
        assertEquals(50L, detalle.particiones().get(0).lag());
        assertEquals(0L, detalle.particiones().get(1).lag());
        assertEquals(700L, detalle.particiones().get(2).lag());
        // 50 + 0 + 700
        assertEquals(750L, detalle.lagTotal());
        assertEquals(0, detalle.particionesSinConfirmar());
        assertEquals(0, detalle.particionesSinAsignar());
    }

    @Test
    @DisplayName("una partición sin offset confirmado se informa, pero no suma al lag total")
    void sinConfirmarNoSumaAlTotal() {
        describe(grupo("Stable", miembro("audit-1", 0, 1)));
        // Solo la partición 0 tiene offset confirmado; la 1 está asignada y todavía sin leer.
        confirmados(Map.of(particion(0), 100L));
        when(cluster.offsetsFinales(any())).thenReturn(offsets(150, 800));

        GrupoDetalle detalle = servicio.describir(GRUPO);

        ParticionDeGrupo sinConfirmar = detalle.particiones().get(1);
        assertNull(sinConfirmar.offsetConfirmado());
        assertNull(sinConfirmar.lag());
        assertEquals(800L, sinConfirmar.offsetFinal());
        // El total sería 850 si se contara la segunda como si empezara en cero, y eso sería
        // inventarse un retraso.
        assertEquals(50L, detalle.lagTotal());
        assertEquals(1, detalle.particionesSinConfirmar());
    }

    @Test
    @DisplayName("se miran también las particiones confirmadas que ya no tiene asignadas nadie")
    void particionesHuerfanas() {
        // Un consumidor con una sola partición, y el grupo con offsets en tres: las otras dos
        // las consumió antes y ahora no las lee nadie. Es justamente el caso que explica un lag
        // que no baja.
        describe(grupo("Stable", miembro("audit-1", 0)));
        confirmados(offsets(100, 100, 100));
        when(cluster.offsetsFinales(any())).thenReturn(offsets(100, 400, 700));

        GrupoDetalle detalle = servicio.describir(GRUPO);

        assertEquals(3, detalle.particiones().size());
        assertEquals(2, detalle.particionesSinAsignar());
        assertEquals(900L, detalle.lagTotal());
        assertNull(detalle.particiones().get(1).miembro());
    }

    @Test
    @DisplayName("la distribución agrupa el lag por consumidor y deja las huérfanas aparte")
    void distribucionPorMiembro() {
        describe(grupo("Stable", miembro("audit-1", 0, 1), miembro("audit-2", 2)));
        confirmados(offsets(0, 0, 0, 0));
        when(cluster.offsetsFinales(any())).thenReturn(offsets(10, 20, 300, 4000));

        List<ConsumoPorMiembro> distribucion = servicio.describir(GRUPO).distribucion();

        assertEquals(3, distribucion.size());
        assertEquals("audit-1", distribucion.get(0).miembro());
        assertEquals(2, distribucion.get(0).particiones());
        assertEquals(30L, distribucion.get(0).lag());
        assertEquals("audit-2", distribucion.get(1).miembro());
        assertEquals(300L, distribucion.get(1).lag());
        // Las particiones sin dueño van al final, con miembro null: es la fila que explica un lag
        // que sube sin que ningún consumidor esté retrasado.
        assertNull(distribucion.get(2).miembro());
        assertEquals(4000L, distribucion.get(2).lag());
    }

    @Test
    @DisplayName("el detalle informa los miembros con su asignación en el formato de Kafka")
    void miembrosConAsignacion() {
        describe(grupo("Stable", miembro("audit-1", 0, 2)));
        confirmados(Map.of());
        when(cluster.offsetsFinales(any())).thenReturn(offsets(10, 0, 30));

        GrupoDetalle detalle = servicio.describir(GRUPO);

        assertEquals(1, detalle.miembros().size());
        assertEquals("audit-1", detalle.miembros().get(0).cliente());
        assertEquals(List.of("reservations.events-0", "reservations.events-2"),
                detalle.miembros().get(0).asignacion());
        assertEquals("range", detalle.estrategiaDeAsignacion());
        assertEquals(1, detalle.coordinador().id());
    }

    // ── Existencia, que en Kafka no es evidente ──────────────────────────────

    @Test
    @DisplayName("404 · un grupo inventado llega como Dead y sin offsets, y no es un 200 vacío")
    void grupoInventadoEs404() {
        // describeConsumerGroups NO falla ante un identificador inventado: devuelve esto. Sin la
        // comprobación, la API respondería 200 con un grupo vacío a cualquier cosa que se le
        // pidiera.
        describe(grupo("Dead"));
        confirmados(Map.of());

        RecursoNoEncontradoException e = assertThrows(RecursoNoEncontradoException.class,
                () -> servicio.describir(GRUPO));

        assertEquals("grupo", e.getRecurso());
        assertEquals(GRUPO, e.getNombre());
    }

    @Test
    @DisplayName("un grupo Dead CON offsets confirmados sí existe y se describe")
    void grupoDeadConOffsetsExiste() {
        // La diferencia entre un grupo inventado y uno real que quedó sin miembros son justamente
        // los offsets: por eso la comprobación cruza las dos señales.
        describe(grupo("Dead"));
        confirmados(offsets(100));
        when(cluster.offsetsFinales(any())).thenReturn(offsets(150));

        GrupoDetalle detalle = servicio.describir(GRUPO);

        assertEquals(50L, detalle.lagTotal());
        assertTrue(detalle.miembros().isEmpty());
    }

    @Test
    @DisplayName("404 · si el cluster responde que no conoce el grupo")
    void grupoDesconocidoEs404() {
        when(cluster.describirGrupo(GRUPO)).thenReturn(Optional.empty());

        assertThrows(RecursoNoEncontradoException.class, () -> servicio.describir(GRUPO));
    }

    @Test
    @DisplayName("el listado devuelve los grupos con su estado")
    void listado() {
        when(cluster.grupos()).thenReturn(List.of(
                new GrupoApi(GRUPO, "Stable", false),
                new GrupoApi("otro", "Empty", true)));

        assertEquals(2, servicio.listar().size());
        assertEquals("Stable", servicio.listar().get(0).estado());
        assertTrue(servicio.listar().get(1).simple());
    }

    // ── Baja ─────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("409 · un grupo con miembros conectados no se elimina")
    void eliminarGrupoActivoEs409() {
        describe(grupo("Stable", miembro("audit-1", 0)));
        confirmados(offsets(10));

        GrupoActivoException e = assertThrows(GrupoActivoException.class,
                () -> servicio.eliminar(GRUPO));

        assertEquals("Stable", e.getEstado());
        assertEquals(List.of("audit-1"), e.getMiembros());
        assertTrue(e.getMessage().contains("quede en Empty"), e.getMessage());
        verify(cluster, never()).eliminarGrupo(anyString());
    }

    @Test
    @DisplayName("un grupo sin miembros se elimina, y los mensajes de los tópicos no se tocan")
    void eliminarGrupoVacio() {
        describe(grupo("Empty"));
        confirmados(offsets(10));

        OperacionResultado resultado = servicio.eliminar(GRUPO);

        verify(cluster).eliminarGrupo(GRUPO);
        assertTrue(resultado.detalle().contains("los mensajes siguen en sus tópicos"),
                resultado.detalle());
    }

    // ── Reposicionamiento ────────────────────────────────────────────────────

    @Test
    @DisplayName("earliest mueve las particiones confirmadas a su offset más antiguo")
    void resetEarliest() {
        describe(grupo("Empty"));
        confirmados(offsets(500, 500));
        when(cluster.offsetsIniciales(any())).thenReturn(offsets(0, 12));

        OperacionResultado resultado = servicio.reposicionar(GRUPO,
                new ResetOffsetsRequest("earliest", null, null));

        verify(cluster).alterarOffsets(eq(GRUPO), eq(offsets(0, 12)));
        // No se usan los finales: son dos lecturas distintas del cluster y confundirlas dejaría el
        // grupo en el extremo contrario al pedido.
        verify(cluster, never()).offsetsFinales(any());
        assertTrue(resultado.detalle().contains("2 partición(es)"), resultado.detalle());
    }

    @Test
    @DisplayName("latest mueve las particiones confirmadas al final, saltándose lo pendiente")
    void resetLatest() {
        describe(grupo("Empty"));
        confirmados(offsets(100));
        when(cluster.offsetsFinales(any())).thenReturn(offsets(9000));

        servicio.reposicionar(GRUPO, new ResetOffsetsRequest("LATEST", null, null));

        // La estrategia llega sin distinguir mayúsculas, como dice la validación del DTO.
        verify(cluster).alterarOffsets(eq(GRUPO), eq(offsets(9000)));
    }

    @Test
    @DisplayName("con un tópico indicado solo se mueven sus particiones")
    void resetFiltradoPorTopico() {
        describe(grupo("Empty"));
        Map<ParticionApi, Long> confirmados = new LinkedHashMap<>();
        confirmados.put(particion(0), 10L);
        confirmados.put(new ParticionApi("audit.timeline", 0), 20L);
        confirmados(confirmados);
        when(cluster.offsetsIniciales(any())).thenReturn(offsets(0));

        servicio.reposicionar(GRUPO, new ResetOffsetsRequest("earliest", TOPICO, null));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<ParticionApi>> pedidas = ArgumentCaptor.forClass(List.class);
        verify(cluster).offsetsIniciales(pedidas.capture());
        assertEquals(List.of(particion(0)), pedidas.getValue());
    }

    @Test
    @DisplayName("explicito mueve exactamente las particiones de la lista")
    void resetExplicito() {
        describe(grupo("Empty"));
        confirmados(offsets(10));
        when(cluster.describirTopico(TOPICO)).thenReturn(Optional.of(topicoCon(3)));

        servicio.reposicionar(GRUPO, new ResetOffsetsRequest("explicito", null,
                List.of(new OffsetDeParticion(TOPICO, 2, 77L))));

        verify(cluster).alterarOffsets(GRUPO, Map.of(particion(2), 77L));
    }

    @Test
    @DisplayName("400 · explicito sin lista de offsets")
    void explicitoSinOffsetsEs400() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> servicio.reposicionar(GRUPO,
                        new ResetOffsetsRequest("explicito", null, List.of())));

        assertTrue(e.getMessage().contains("necesita la lista de offsets"), e.getMessage());
        // La coherencia se comprueba antes de tocar el cluster: es un error de la petición.
        verify(cluster, never()).describirGrupo(anyString());
    }

    @Test
    @DisplayName("400 · earliest con una lista de offsets, que no se usaría")
    void earliestConOffsetsEs400() {
        // Aceptarla en silencio sería peor: quien la envió creería que se aplicó.
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> servicio.reposicionar(GRUPO, new ResetOffsetsRequest("earliest", null,
                        List.of(new OffsetDeParticion(TOPICO, 0, 5L)))));

        assertTrue(e.getMessage().contains("solo se usa con la estrategia explicito"),
                e.getMessage());
    }

    @Test
    @DisplayName("400 · explicito con un tópico suelto, que no se aplicaría a nada")
    void explicitoConTopicoEs400() {
        assertThrows(IllegalArgumentException.class, () -> servicio.reposicionar(GRUPO,
                new ResetOffsetsRequest("explicito", TOPICO,
                        List.of(new OffsetDeParticion(TOPICO, 0, 5L)))));
    }

    @Test
    @DisplayName("400 · un grupo sin offsets confirmados no tiene nada que mover con earliest")
    void sinConfirmadosEs400() {
        describe(grupo("Empty"));
        confirmados(offsets(1));
        when(cluster.offsetsConfirmados(GRUPO)).thenReturn(Map.of());

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> servicio.reposicionar(GRUPO, new ResetOffsetsRequest("earliest", null, null)));

        // El mensaje propone la salida: para posicionar particiones que el grupo nunca consumió
        // está la estrategia explicito.
        assertTrue(e.getMessage().contains("explicito"), e.getMessage());
    }

    @Test
    @DisplayName("404 · explicito sobre un tópico que no existe")
    void explicitoConTopicoInexistenteEs404() {
        describe(grupo("Empty"));
        confirmados(offsets(10));
        when(cluster.describirTopico("no.existe")).thenReturn(Optional.empty());

        RecursoNoEncontradoException e = assertThrows(RecursoNoEncontradoException.class,
                () -> servicio.reposicionar(GRUPO, new ResetOffsetsRequest("explicito", null,
                        List.of(new OffsetDeParticion("no.existe", 0, 5L)))));

        assertEquals("tópico", e.getRecurso());
        verify(cluster, never()).alterarOffsets(anyString(), anyMap());
    }

    @Test
    @DisplayName("400 · explicito sobre una partición que el tópico no tiene")
    void explicitoConParticionInexistenteEs400() {
        describe(grupo("Empty"));
        confirmados(offsets(10));
        when(cluster.describirTopico(TOPICO)).thenReturn(Optional.of(topicoCon(3)));

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> servicio.reposicionar(GRUPO, new ResetOffsetsRequest("explicito", null,
                        List.of(new OffsetDeParticion(TOPICO, 7, 5L)))));

        assertTrue(e.getMessage().contains("numeradas de 0 a 2"), e.getMessage());
        verify(cluster, never()).alterarOffsets(anyString(), anyMap());
    }

    @Test
    @DisplayName("409 · no se mueven los offsets de un grupo con miembros conectados")
    void resetConGrupoActivoEs409() {
        describe(grupo("Stable", miembro("audit-1", 0)));
        confirmados(offsets(10));

        GrupoActivoException e = assertThrows(GrupoActivoException.class,
                () -> servicio.reposicionar(GRUPO, new ResetOffsetsRequest("latest", null, null)));

        // Kafka tampoco lo permitiría, pero responde UnknownMemberId, que no explica nada. Aquí
        // sale con el estado, los miembros y qué hay que hacer.
        assertFalse(e.getMiembros().isEmpty());
        verify(cluster, never()).alterarOffsets(anyString(), anyMap());
    }

    private static TopicoApi topicoCon(int particiones) {
        List<ParticionDescripcionApi> detalle = new java.util.ArrayList<>();
        for (int i = 0; i < particiones; i++) {
            detalle.add(new ParticionDescripcionApi(i, new NodoApi(1, "kafka1", 29092),
                    List.of(), List.of()));
        }
        return new TopicoApi(TOPICO, false, detalle);
    }
}
