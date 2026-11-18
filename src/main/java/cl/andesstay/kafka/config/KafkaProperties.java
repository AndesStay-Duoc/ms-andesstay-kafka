package cl.andesstay.kafka.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.util.List;

/**
 * Configuracion de administracion del cluster, tipada y en un solo sitio.
 *
 * Esta clase existe para que la configuracion no se reparta en @Value sueltos por los servicios:
 * la logica de negocio recibe un objeto validado y no sabe de donde salieron los valores. Con
 * @Value repartidos, cambiar el nombre de una propiedad obliga a buscarla por todo el codigo y un
 * error solo aparece al arrancar; aqui el contrato esta en un sitio y se valida al construirlo.
 *
 * La conexion del PRODUCTOR de la prueba de carga no vive aqui: esa la lee Spring Boot de
 * spring.kafka.*, porque el KafkaTemplate lo construye su autoconfiguracion. Las dos salen de la
 * misma variable de entorno, KAFKA_BOOTSTRAP, y estan escritas por separado porque son dos
 * clientes distintos del cluster.
 *
 * @param bootstrapServers lista de brokers desde la que el AdminClient descubre el cluster
 * @param timeoutMs        limite por operacion del AdminClient; ver KafkaAdminConfig
 */
@Validated
@ConfigurationProperties("andesstay.kafka")
public record KafkaProperties(

        @NotBlank String bootstrapServers,

        @Positive int timeoutMs,

        /**
         * Topicos que esta API se niega a ELIMINAR. Son la topologia de AndesStay.
         *
         * Eliminar un topico en Kafka es irreversible: se va el log de todas sus particiones en
         * los tres brokers, y con el los offsets de los grupos que lo consumian. Un DELETE
         * accidental sobre reservations.events corta el flujo de eventos hacia ms-andesstay-audit
         * y no hay forma de recuperar lo que hubiera dentro.
         *
         * La proteccion cubre el borrado y no las configuraciones: alterar retention.ms de un
         * topico protegido es reversible y es justo lo que la evaluacion pide poder demostrar.
         *
         * No se expone como variable de entorno a proposito: desactivar el seguro desde la
         * configuracion del despliegue seria perder justo lo que aporta.
         */
        List<String> protegidos,

        /** Valores por omision de la prueba de carga del perfil demo. */
        Pruebas pruebas
) {

    /**
     * @param topico   topico sobre el que publica la prueba de carga si no se indica otro
     * @param mensajes cuantos mensajes publica si no se indica otra cantidad
     */
    public record Pruebas(String topico, int mensajes) {
    }

    /** Indica si el nombre esta en la lista de protegidos. */
    public boolean esProtegido(String nombre) {
        return protegidos != null && protegidos.contains(nombre);
    }

    /** El limite por operacion, ya como Duration: es lo que esperan los futuros del AdminClient. */
    public Duration timeout() {
        return Duration.ofMillis(timeoutMs);
    }
}
