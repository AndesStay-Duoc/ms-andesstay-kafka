package cl.andesstay.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * El AdminClient del cluster. Aqui no hay ni una regla de negocio: esta clase solo construye el
 * cliente, y quien lo usa es broker/KafkaAdminClient.
 *
 * POR QUE EL ADMINCLIENT Y NO OTRA COSA. Kafka no tiene una API HTTP de administracion como la que
 * RabbitMQ publica en el 15672; lo que hay es el protocolo binario de los propios brokers, y el
 * cliente oficial de ese protocolo es el AdminClient de kafka-clients. Por el pasan tanto las
 * escrituras (createTopics, incrementalAlterConfigs, deleteTopics) como las lecturas (listTopics,
 * describeConfigs, listConsumerGroupOffsets), asi que este servicio habla con el cluster por una
 * sola via, al contrario que ms-andesstay-mq, que necesita dos.
 *
 * Construirlo NO abre conexion: el AdminClient conecta de forma perezosa en la primera operacion.
 * Por eso el servicio arranca aunque el cluster este caido, y el fallo aparece como un 503 en la
 * primera peticion en lugar de impedir el arranque. Es lo que se quiere en un administrador: tiene
 * que poder arrancar para poder diagnosticar.
 */
@Slf4j
@Configuration
public class KafkaAdminConfig {

    /**
     * Los dos timeouts son el motivo de construir el cliente a mano en lugar de dejarselo a la
     * autoconfiguracion de spring-kafka.
     *
     * Sin ellos, el AdminClient reintenta contra un cluster inalcanzable durante los dos minutos
     * de su default.api.timeout.ms, y durante todo ese rato retiene el hilo de Tomcat que atiende
     * la peticion. Con diez segundos, la peticion falla rapido y el @RestControllerAdvice la
     * traduce a un 503.
     *
     * request.timeout.ms limita cada intento contra un broker; default.api.timeout.ms limita la
     * operacion completa, reintentos incluidos. Se fijan los dos al mismo valor: lo que interesa
     * es el limite de arriba, y dejar el de abajo en su valor por omision (30 s, mas alto que el
     * de arriba) haria que el de arriba no se pudiera respetar.
     */
    @Bean(destroyMethod = "close")
    public Admin adminClient(KafkaProperties propiedades) {
        Map<String, Object> configuracion = new HashMap<>();
        configuracion.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, propiedades.bootstrapServers());
        configuracion.put(AdminClientConfig.CLIENT_ID_CONFIG, "ms-andesstay-kafka-admin");
        configuracion.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, propiedades.timeoutMs());
        configuracion.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, propiedades.timeoutMs());

        log.info("[Cluster] AdminClient configurado contra {} con un limite de {} ms por operacion",
                propiedades.bootstrapServers(), propiedades.timeoutMs());
        return AdminClient.create(configuracion);
    }
}
