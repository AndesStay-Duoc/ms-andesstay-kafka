package cl.andesstay.kafka.broker;

import cl.andesstay.kafka.config.KafkaProperties;
import cl.andesstay.kafka.web.ApiDeGestionException;
import cl.andesstay.kafka.web.GrupoActivoException;
import cl.andesstay.kafka.web.RecursoNoEncontradoException;
import cl.andesstay.kafka.web.RecursoYaExisteException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.clients.admin.TopicDescription;
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * La traducción de lo que devuelve el AdminClient, sin cluster.
 *
 * Es la lógica más frágil del servicio y la que no se comprueba leyendo el código: el motivo por
 * el que el cluster rechazó una operación no viaja en el mensaje de la excepción, sino en la
 * CAUSA de una ExecutionException, y el tipo de esa causa es lo único que distingue «no existe»
 * de «ya existe» o de «el cluster no está». Si ese recorrido se rompe, los casos normales de una
 * API de administración vuelven a salir como 500 y nada más falla.
 *
 * Por eso aquí se arman a mano los futuros fallidos, con la misma forma en que los entrega el
 * cliente de Kafka.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaAdminClientTest {

    private static final String TOPICO = "reservations.events";
    private static final String GRUPO = "andesstay-audit";

    @Mock
    private Admin admin;

    private KafkaAdminClient cliente;

    @BeforeEach
    void preparar() {
        // Las propiedades son un record, así que se construyen en lugar de simularse: un mock de
        // un objeto de configuración solo añadiría ruido.
        KafkaProperties propiedades = new KafkaProperties(
                "localhost:9092", 2_000, List.of(TOPICO),
                new KafkaProperties.Pruebas("pruebas.carga", 10_000));
        cliente = new KafkaAdminClient(admin, propiedades);
    }

    /** Un futuro que falla con la causa indicada, como lo entrega el AdminClient. */
    private static <T> KafkaFuture<T> fallido(Throwable causa) {
        KafkaFutureImpl<T> futuro = new KafkaFutureImpl<>();
        futuro.completeExceptionally(causa);
        return futuro;
    }

    private static Node nodo(int id) {
        return new Node(id, "kafka" + id, 29090 + id);
    }

    private static TopicDescription descripcion(String nombre, int particiones, int replicas,
                                                int isr) {
        List<TopicPartitionInfo> info = new java.util.ArrayList<>();
        for (int i = 0; i < particiones; i++) {
            info.add(new TopicPartitionInfo(i, nodo(1),
                    nodosHasta(replicas), nodosHasta(isr)));
        }
        return new TopicDescription(nombre, false, info);
    }

    private static List<Node> nodosHasta(int cuantos) {
        List<Node> nodos = new java.util.ArrayList<>();
        for (int i = 1; i <= cuantos; i++) {
            nodos.add(nodo(i));
        }
        return nodos;
    }

    // ── Altas ────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("crear un tópico envía el nombre, las particiones, el factor y las configuraciones")
    void crearEnviaLoPedido() {
        CreateTopicsResult resultado = mock(CreateTopicsResult.class);
        when(resultado.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.createTopics(anyCollection())).thenReturn(resultado);

        cliente.crearTopico("pruebas.demo", 6, (short) 3, Map.of("retention.ms", "60000"));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<NewTopic>> enviado = ArgumentCaptor.forClass(Collection.class);
        verify(admin).createTopics(enviado.capture());

        NewTopic topico = enviado.getValue().iterator().next();
        assertEquals("pruebas.demo", topico.name());
        assertEquals(6, topico.numPartitions());
        assertEquals((short) 3, topico.replicationFactor());
        // Las configuraciones se pasan tal cual: validarlas aquí sería duplicar la tabla del
        // cluster y quedarse atrás.
        assertEquals(Map.of("retention.ms", "60000"), topico.configs());
    }

    @Test
    @DisplayName("sin configuraciones, el tópico no lleva ninguna fijada y hereda las del broker")
    void crearSinConfiguraciones() {
        CreateTopicsResult resultado = mock(CreateTopicsResult.class);
        when(resultado.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.createTopics(anyCollection())).thenReturn(resultado);

        cliente.crearTopico("pruebas.demo", 3, (short) 3, Map.of());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<NewTopic>> enviado = ArgumentCaptor.forClass(Collection.class);
        verify(admin).createTopics(enviado.capture());
        assertNull(enviado.getValue().iterator().next().configs());
    }

    @Test
    @DisplayName("409 · un tópico que ya existe no es un alta idempotente, al contrario que en AMQP")
    void topicoRepetidoEs409() {
        CreateTopicsResult resultado = mock(CreateTopicsResult.class);
        when(resultado.all()).thenReturn(fallido(new TopicExistsException("Topic 'x' already exists.")));
        when(admin.createTopics(anyCollection())).thenReturn(resultado);

        RecursoYaExisteException e = assertThrows(RecursoYaExisteException.class,
                () -> cliente.crearTopico("pruebas.demo", 3, (short) 3, Map.of()));

        assertEquals("pruebas.demo", e.getNombre());
        // El mensaje tiene que advertir de la diferencia con RabbitMQ, o quien escriba un script
        // idempotente se lo encontrará en la segunda ejecución.
        assertTrue(e.getMessage().contains("aunque la definición coincida"), e.getMessage());
    }

    @Test
    @DisplayName("400 · pedir más réplicas que brokers hay es un error de quien llama, no un 500")
    void factorImposibleEs400() {
        CreateTopicsResult resultado = mock(CreateTopicsResult.class);
        when(resultado.all()).thenReturn(fallido(new InvalidReplicationFactorException(
                "Replication factor: 9 larger than available brokers: 3.")));
        when(admin.createTopics(anyCollection())).thenReturn(resultado);

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> cliente.crearTopico("pruebas.demo", 3, (short) 9, Map.of()));

        // El texto del cluster nombra el número concreto, que es más útil que cualquier mensaje
        // propio.
        assertTrue(e.getMessage().contains("larger than available brokers"), e.getMessage());
    }

    // ── Bajas y existencia ───────────────────────────────────────────────────

    @Test
    @DisplayName("404 · eliminar un tópico inexistente, con el recurso y el nombre señalados")
    void eliminarInexistenteEs404() {
        DeleteTopicsResult resultado = mock(DeleteTopicsResult.class);
        when(resultado.all()).thenReturn(fallido(new UnknownTopicOrPartitionException("nope")));
        when(admin.deleteTopics(anyCollection())).thenReturn(resultado);

        RecursoNoEncontradoException e = assertThrows(RecursoNoEncontradoException.class,
                () -> cliente.eliminarTopico("no.existe"));

        assertEquals("tópico", e.getRecurso());
        assertEquals("no.existe", e.getNombre());
    }

    @Test
    @DisplayName("describir un tópico inexistente devuelve vacío, no un error")
    void describirInexistenteEsVacio() {
        DescribeTopicsResult resultado = mock(DescribeTopicsResult.class);
        when(resultado.topicNameValues()).thenReturn(
                Map.of("no.existe", fallido(new UnknownTopicOrPartitionException("nope"))));
        when(admin.describeTopics(anyCollection())).thenReturn(resultado);

        assertTrue(cliente.describirTopico("no.existe").isEmpty());
    }

    @Test
    @DisplayName("el listado omite el tópico que desapareció entre listar y describir")
    void listadoToleraLaCarrera() {
        DescribeTopicsResult resultado = mock(DescribeTopicsResult.class);
        when(resultado.topicNameValues()).thenReturn(Map.of(
                TOPICO, KafkaFuture.completedFuture(descripcion(TOPICO, 3, 3, 3)),
                "se.fue", fallido(new UnknownTopicOrPartitionException("nope"))));
        when(admin.describeTopics(anyCollection())).thenReturn(resultado);

        Map<String, KafkaAdminClient.TopicoApi> topicos =
                cliente.describirTopicos(List.of(TOPICO, "se.fue"));

        // Lo esencial: una carrera normal en un cluster que alguien está administrando no puede
        // tirar la respuesta entera.
        assertEquals(Set.of(TOPICO), topicos.keySet());
        assertEquals(3, topicos.get(TOPICO).particiones().size());
    }

    @Test
    @DisplayName("el listado de nombres pide también los internos; el filtro es del servicio")
    void listadoPideLosInternos() {
        ListTopicsResult resultado = mock(ListTopicsResult.class);
        when(resultado.names()).thenReturn(
                KafkaFuture.completedFuture(Set.of("b.topico", "a.topico", "__consumer_offsets")));
        when(admin.listTopics(any(ListTopicsOptions.class))).thenReturn(resultado);

        Set<String> nombres = cliente.nombresDeTopicos();

        ArgumentCaptor<ListTopicsOptions> opciones =
                ArgumentCaptor.forClass(ListTopicsOptions.class);
        verify(admin).listTopics(opciones.capture());
        assertTrue(opciones.getValue().shouldListInternal(),
                "los internos se piden siempre: el criterio de qué se esconde vive en el servicio");
        // Llegan ordenados, que es lo que el listado necesita.
        assertEquals(List.of("__consumer_offsets", "a.topico", "b.topico"), List.copyOf(nombres));
    }

    // ── Configuración ────────────────────────────────────────────────────────

    @Test
    @DisplayName("modificar configuraciones usa el alter INCREMENTAL, que no borra las demás")
    void configuracionIncremental() {
        AlterConfigsResult resultado = mock(AlterConfigsResult.class);
        when(resultado.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.incrementalAlterConfigs(anyMap())).thenReturn(resultado);

        cliente.alterarConfiguraciones(TOPICO, Map.of("retention.ms", "604800000"));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<ConfigResource, Collection<AlterConfigOp>>> enviado =
                ArgumentCaptor.forClass(Map.class);
        verify(admin).incrementalAlterConfigs(enviado.capture());

        ConfigResource recurso = enviado.getValue().keySet().iterator().next();
        assertEquals(ConfigResource.Type.TOPIC, recurso.type());
        assertEquals(TOPICO, recurso.name());

        AlterConfigOp operacion = enviado.getValue().get(recurso).iterator().next();
        // SET y no APPEND: lo que se pide es fijar el valor de esa clave.
        assertEquals(AlterConfigOp.OpType.SET, operacion.opType());
        assertEquals("604800000", operacion.configEntry().value());
    }

    @Test
    @DisplayName("un valor null quita la clave y la devuelve a su valor por omisión")
    void valorNuloBorraLaClave() {
        AlterConfigsResult resultado = mock(AlterConfigsResult.class);
        when(resultado.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.incrementalAlterConfigs(anyMap())).thenReturn(resultado);

        Map<String, String> cambios = new java.util.LinkedHashMap<>();
        cambios.put("retention.ms", null);
        cliente.alterarConfiguraciones(TOPICO, cambios);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<ConfigResource, Collection<AlterConfigOp>>> enviado =
                ArgumentCaptor.forClass(Map.class);
        verify(admin).incrementalAlterConfigs(enviado.capture());

        AlterConfigOp operacion = enviado.getValue().values().iterator().next().iterator().next();
        // Es la única forma de QUITAR una configuración en lugar de fijarla.
        assertEquals(AlterConfigOp.OpType.DELETE, operacion.opType());
    }

    @Test
    @DisplayName("la configuración efectiva distingue lo fijado en el tópico de lo heredado")
    void configuracionDistingueElOrigen() {
        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, TOPICO);
        Config configuracion = new Config(List.of(
                new ConfigEntry("retention.ms", "604800000",
                        ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG, false, false,
                        List.of(), ConfigEntry.ConfigType.LONG, null),
                new ConfigEntry("cleanup.policy", "delete",
                        ConfigEntry.ConfigSource.DEFAULT_CONFIG, false, true,
                        List.of(), ConfigEntry.ConfigType.LIST, null)));

        DescribeConfigsResult resultado = mock(DescribeConfigsResult.class);
        when(resultado.all()).thenReturn(KafkaFuture.completedFuture(Map.of(recurso, configuracion)));
        when(admin.describeConfigs(anyCollection())).thenReturn(resultado);

        List<KafkaAdminClient.ConfiguracionApi> claves = cliente.configuracionDeTopico(TOPICO);

        // Vienen ordenadas por nombre: cleanup.policy antes que retention.ms.
        assertEquals("cleanup.policy", claves.get(0).nombre());
        assertTrue(claves.get(0).porOmision(), "lo heredado del broker no es propio del tópico");
        assertFalse(claves.get(1).porOmision(), "lo fijado sobre el tópico sí es propio");
    }

    // ── Particiones ──────────────────────────────────────────────────────────

    @Test
    @DisplayName("aumentar particiones envía el TOTAL, no cuántas se agregan")
    void particionesEnviaElTotal() {
        CreatePartitionsResult resultado = mock(CreatePartitionsResult.class);
        when(resultado.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.createPartitions(anyMap())).thenReturn(resultado);

        cliente.crearParticiones(TOPICO, 6);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, NewPartitions>> enviado = ArgumentCaptor.forClass(Map.class);
        verify(admin).createPartitions(enviado.capture());
        assertEquals(6, enviado.getValue().get(TOPICO).totalCount());
    }

    // ── Grupos ───────────────────────────────────────────────────────────────

    @Test
    @DisplayName("el listado de grupos usa valid(), para no quedarse sin respuesta por un broker caído")
    void grupoListadoTolerante() {
        ListConsumerGroupsResult resultado = mock(ListConsumerGroupsResult.class);
        when(resultado.valid()).thenReturn(KafkaFuture.completedFuture(List.of(
                new ConsumerGroupListing("zeta", false, Optional.of(ConsumerGroupState.EMPTY)),
                new ConsumerGroupListing(GRUPO, false, Optional.of(ConsumerGroupState.STABLE)))));
        when(admin.listConsumerGroups()).thenReturn(resultado);

        List<KafkaAdminClient.GrupoApi> grupos = cliente.grupos();

        assertEquals(GRUPO, grupos.get(0).id(), "tiene que venir ordenado por id");
        // El estado se informa como lo escriben la consola de Kafka y Kafka UI, no como se llama
        // el enum: Stable, no STABLE.
        assertEquals("Stable", grupos.get(0).estado());
        assertEquals("Empty", grupos.get(1).estado());
    }

    @Test
    @DisplayName("describir un grupo trae sus miembros con la asignación de cada uno")
    void describirGrupoTraeLaAsignacion() {
        MemberDescription miembro = new MemberDescription("consumidor-1", "ms-andesstay-audit",
                "10.0.0.5", new MemberAssignment(Set.of(new TopicPartition(TOPICO, 0))));
        ConsumerGroupDescription descripcion = new ConsumerGroupDescription(GRUPO, false,
                List.of(miembro), "range", ConsumerGroupState.STABLE, nodo(1));

        DescribeConsumerGroupsResult resultado = mock(DescribeConsumerGroupsResult.class);
        when(resultado.all()).thenReturn(KafkaFuture.completedFuture(Map.of(GRUPO, descripcion)));
        when(admin.describeConsumerGroups(anyCollection())).thenReturn(resultado);

        KafkaAdminClient.GrupoDescripcionApi grupo = cliente.describirGrupo(GRUPO).orElseThrow();

        assertEquals("Stable", grupo.estado());
        assertEquals("range", grupo.estrategiaDeAsignacion());
        assertEquals(1, grupo.coordinador().id());
        assertEquals("ms-andesstay-audit", grupo.miembros().get(0).cliente());
        assertEquals(new KafkaAdminClient.ParticionApi(TOPICO, 0),
                grupo.miembros().get(0).asignacion().get(0));
    }

    @Test
    @DisplayName("un GroupIdNotFound llega como vacío, no como error")
    void grupoDesconocidoEsVacio() {
        DescribeConsumerGroupsResult resultado = mock(DescribeConsumerGroupsResult.class);
        when(resultado.all()).thenReturn(fallido(new GroupIdNotFoundException("nope")));
        when(admin.describeConsumerGroups(anyCollection())).thenReturn(resultado);

        assertTrue(cliente.describirGrupo("inventado").isEmpty());
    }

    @Test
    @DisplayName("los offsets sin confirmar no se inventan: la partición no aparece en el mapa")
    void offsetsSinConfirmarNoAparecen() {
        Map<TopicPartition, OffsetAndMetadata> confirmados = new java.util.LinkedHashMap<>();
        confirmados.put(new TopicPartition(TOPICO, 0), new OffsetAndMetadata(120));
        confirmados.put(new TopicPartition(TOPICO, 1), null);

        ListConsumerGroupOffsetsResult resultado = mock(ListConsumerGroupOffsetsResult.class);
        when(resultado.partitionsToOffsetAndMetadata())
                .thenReturn(KafkaFuture.completedFuture(confirmados));
        when(admin.listConsumerGroupOffsets(anyString())).thenReturn(resultado);

        Map<KafkaAdminClient.ParticionApi, Long> offsets = cliente.offsetsConfirmados(GRUPO);

        assertEquals(1, offsets.size());
        assertEquals(120L, offsets.get(new KafkaAdminClient.ParticionApi(TOPICO, 0)));
    }

    @Test
    @DisplayName("los offsets finales se leen con listOffsets y vuelven por partición")
    void offsetsFinales() {
        ListOffsetsResult resultado = mock(ListOffsetsResult.class);
        when(resultado.all()).thenReturn(KafkaFuture.completedFuture(Map.of(
                new TopicPartition(TOPICO, 0),
                new ListOffsetsResult.ListOffsetsResultInfo(500, 0L, Optional.empty()))));
        when(admin.listOffsets(anyMap())).thenReturn(resultado);

        Map<KafkaAdminClient.ParticionApi, Long> finales = cliente.offsetsFinales(
                List.of(new KafkaAdminClient.ParticionApi(TOPICO, 0)));

        assertEquals(500L, finales.get(new KafkaAdminClient.ParticionApi(TOPICO, 0)));
    }

    @Test
    @DisplayName("sin particiones que consultar no se molesta al cluster")
    void sinParticionesNoHayLlamada() {
        assertTrue(cliente.offsetsFinales(List.of()).isEmpty());
        verify(admin, org.mockito.Mockito.never()).listOffsets(anyMap());
    }

    @Nested
    @DisplayName("los errores que Kafka devuelve con un nombre que no explica nada")
    class ErroresOpacos {

        @Test
        @DisplayName("409 · GroupNotEmpty al eliminar un grupo en marcha")
        void grupoNoVacioEs409() {
            DeleteConsumerGroupsResult resultado = mock(DeleteConsumerGroupsResult.class);
            when(resultado.all()).thenReturn(fallido(new GroupNotEmptyException("not empty")));
            when(admin.deleteConsumerGroups(anyCollection())).thenReturn(resultado);

            GrupoActivoException e = assertThrows(GrupoActivoException.class,
                    () -> cliente.eliminarGrupo(GRUPO));

            assertEquals(GRUPO, e.getGrupo());
            assertTrue(e.getMessage().contains("quede en Empty"), e.getMessage());
        }

        @Test
        @DisplayName("409 · UnknownMemberId al mover los offsets de un grupo con miembros")
        void miembroDesconocidoEs409() {
            AlterConsumerGroupOffsetsResult resultado =
                    mock(AlterConsumerGroupOffsetsResult.class);
            when(resultado.all()).thenReturn(fallido(new UnknownMemberIdException("unknown")));
            when(admin.alterConsumerGroupOffsets(anyString(), anyMap())).thenReturn(resultado);

            // Es el error más desconcertante de Kafka: UnknownMemberId no dice «el grupo está en
            // marcha» por ningún lado, y es exactamente lo que significa aquí.
            assertThrows(GrupoActivoException.class,
                    () -> cliente.alterarOffsets(GRUPO, Map.of(
                            new KafkaAdminClient.ParticionApi(TOPICO, 0), 0L)));
        }

        @Test
        @DisplayName("503 · un timeout del cluster se marca como inalcanzable, no como rechazo")
        void timeoutEsInalcanzable() {
            ListTopicsResult resultado = mock(ListTopicsResult.class);
            when(resultado.names()).thenReturn(fallido(
                    new org.apache.kafka.common.errors.TimeoutException("expired")));
            when(admin.listTopics(any(ListTopicsOptions.class))).thenReturn(resultado);

            ApiDeGestionException e = assertThrows(ApiDeGestionException.class,
                    () -> cliente.nombresDeTopicos());

            // Es lo que separa el 503 del 502: el cluster no contestó, no es que rechazara.
            assertFalse(e.esAlcanzable());
        }

        @Test
        @DisplayName("502 · un fallo inesperado no se disfraza de 404 ni de 400")
        void inesperadoEsRechazo() {
            ListTopicsResult resultado = mock(ListTopicsResult.class);
            when(resultado.names()).thenReturn(fallido(new IllegalStateException("vaya")));
            when(admin.listTopics(any(ListTopicsOptions.class))).thenReturn(resultado);

            ApiDeGestionException e = assertThrows(ApiDeGestionException.class,
                    () -> cliente.nombresDeTopicos());

            assertTrue(e.esAlcanzable(), "un fallo del que no se sabe nada no es un cluster caído");
        }

        @Test
        @DisplayName("un «no existe» en una operación que no trata sobre un recurso sale como 502")
        void noEncontradoSinRecursoEsRechazo() {
            // Sin un recurso concreto al que referirse, un 404 no significaría nada para quien
            // llama: el listado no pidió ningún tópico en particular.
            ListTopicsResult resultado = mock(ListTopicsResult.class);
            when(resultado.names()).thenReturn(
                    fallido(new UnknownTopicOrPartitionException("nope")));
            when(admin.listTopics(any(ListTopicsOptions.class))).thenReturn(resultado);

            assertThrows(ApiDeGestionException.class, () -> cliente.nombresDeTopicos());
        }
    }

    @Test
    @DisplayName("el reposicionamiento envía los offsets pedidos al grupo pedido")
    void alterarOffsetsEnviaLoPedido() {
        AlterConsumerGroupOffsetsResult resultado = mock(AlterConsumerGroupOffsetsResult.class);
        when(resultado.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.alterConsumerGroupOffsets(anyString(), anyMap())).thenReturn(resultado);

        cliente.alterarOffsets(GRUPO, Map.of(new KafkaAdminClient.ParticionApi(TOPICO, 2), 77L));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> enviado =
                ArgumentCaptor.forClass(Map.class);
        verify(admin).alterConsumerGroupOffsets(eq(GRUPO), enviado.capture());
        assertEquals(77L, enviado.getValue().get(new TopicPartition(TOPICO, 2)).offset());
    }
}
