package cl.andesstay.kafka.config;

import cl.andesstay.kafka.broker.KafkaAdminClient;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Salud del servicio: si no alcanza el cluster, no sirve para nada.
 *
 * Es el mismo criterio que toma ms-andesstay-mq al dejar activo management.health.rabbit: un
 * administrador sin broker no tiene nada que administrar, asi que el contenedor debe quedar
 * unhealthy y no recibir trafico. Spring Boot no trae un indicador de salud para Kafka, de ahi
 * que este escrito a mano.
 *
 * Se usa describeCluster y no una operacion mas barata porque es la unica que confirma las dos
 * cosas que importan: que hay conexion y que el cluster tiene controlador elegido. Un cluster que
 * responde pero no tiene controlador no acepta creaciones ni borrados de topicos.
 */
@Component
@RequiredArgsConstructor
public class KafkaHealthIndicator implements HealthIndicator {

    private final KafkaAdminClient cluster;

    @Override
    public Health health() {
        try {
            KafkaAdminClient.ClusterApi descripcion = cluster.describirCluster();
            Health.Builder estado = descripcion.controlador() == null
                    ? Health.down().withDetail("motivo", "el cluster no tiene controlador elegido")
                    : Health.up();

            return estado
                    .withDetail("cluster", descripcion.id())
                    .withDetail("brokers", descripcion.nodos().size())
                    .withDetail("controlador", descripcion.controlador() == null
                            ? "ninguno"
                            : descripcion.controlador().id())
                    .build();

        } catch (RuntimeException e) {
            return Health.down(e).build();
        }
    }
}
