package cl.andesstay.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * ms-andesstay-kafka. API REST de administracion de la plataforma Kafka: crea, modifica y elimina
 * topicos y configuraciones, e inspecciona los grupos de consumidores y su lag.
 *
 * Es un servicio de administracion, no de dominio: no consume ni publica eventos de negocio y no
 * declara la topologia de AndesStay. Los topicos del sistema los crea infra/kafka/compose.yml al
 * levantar el cluster, y los productores y consumidores reales viven en ms-andesstay-reservations
 * y ms-andesstay-audit. Aqui solo se administra lo que llega por HTTP.
 *
 * La unica excepcion es la prueba de carga del perfil demo, que si publica mensajes: existe para
 * generar lag visible durante la demostracion, y por eso esta detras de un perfil que no se activa
 * en un despliegue normal.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaAdminApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaAdminApplication.class, args);
    }
}
