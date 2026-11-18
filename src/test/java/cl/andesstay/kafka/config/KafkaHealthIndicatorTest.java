package cl.andesstay.kafka.config;

import cl.andesstay.kafka.broker.KafkaAdminClient;
import cl.andesstay.kafka.web.ApiDeGestionException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

/**
 * La salud del servicio, que decide si el contenedor recibe tráfico.
 *
 * Un administrador que no alcanza el cluster no tiene nada que administrar, así que tiene que
 * quedar unhealthy. Es el mismo criterio que toma ms-andesstay-mq con la salud de RabbitMQ.
 */
@ExtendWith(MockitoExtension.class)
class KafkaHealthIndicatorTest {

    @Mock
    private KafkaAdminClient cluster;

    @InjectMocks
    private KafkaHealthIndicator indicador;

    private static KafkaAdminClient.NodoApi nodo(int id) {
        return new KafkaAdminClient.NodoApi(id, "kafka" + id, 29090 + id);
    }

    @Test
    @DisplayName("UP · con el cluster respondiendo y su controlador elegido")
    void clusterSanoEsUp() {
        when(cluster.describirCluster()).thenReturn(new KafkaAdminClient.ClusterApi(
                "andesstay-cluster", List.of(nodo(1), nodo(2), nodo(3)), nodo(1)));

        Health salud = indicador.health();

        assertEquals(Status.UP, salud.getStatus());
        assertEquals(3, salud.getDetails().get("brokers"));
        assertEquals("andesstay-cluster", salud.getDetails().get("cluster"));
    }

    @Test
    @DisplayName("DOWN · un cluster sin controlador no acepta crear ni borrar tópicos")
    void sinControladorEsDown() {
        // Responde, pero no sirve para administrar: es exactamente el caso que un health check
        // trivial de «hay conexión» dejaría pasar como sano.
        when(cluster.describirCluster()).thenReturn(new KafkaAdminClient.ClusterApi(
                "andesstay-cluster", List.of(nodo(1)), null));

        Health salud = indicador.health();

        assertEquals(Status.DOWN, salud.getStatus());
        assertEquals("ninguno", salud.getDetails().get("controlador"));
    }

    @Test
    @DisplayName("DOWN · si el cluster no responde")
    void clusterCaidoEsDown() {
        when(cluster.describirCluster())
                .thenThrow(new ApiDeGestionException("sin respuesta", false, null));

        assertEquals(Status.DOWN, indicador.health().getStatus());
    }
}
