package cl.andesstay.kafka.web;

/**
 * La administración del cluster no respondió como se esperaba.
 *
 * Envuelve las excepciones del AdminClient para que los servicios no dependan de tipos de
 * org.apache.kafka: la lógica de negocio solo tiene que saber que la operación no se pudo
 * completar contra el cluster, no con qué cliente se intentó. La traducción vive entera en
 * broker/KafkaAdminClient.
 *
 * @param alcanzable false cuando la petición venció sin respuesta del cluster, que es el caso de
 *                   los brokers caídos o de un bootstrap que no resuelve. Lo usa el manejador de
 *                   errores para distinguir un 503 de un 502.
 */
public class ApiDeGestionException extends RuntimeException {

    private final boolean alcanzable;

    public ApiDeGestionException(String mensaje, boolean alcanzable, Throwable causa) {
        super(mensaje, causa);
        this.alcanzable = alcanzable;
    }

    public boolean esAlcanzable() {
        return alcanzable;
    }
}
