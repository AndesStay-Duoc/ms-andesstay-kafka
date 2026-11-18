package cl.andesstay.kafka.web;

/**
 * 409 · El tópico que se quiso crear ya existe.
 *
 * Al contrario que `queue.declare` de AMQP, que es idempotente y vuelve a responder bien ante una
 * redeclaración idéntica, `createTopics` de Kafka falla siempre que el nombre ya está tomado, sin
 * mirar si las particiones y la configuración coinciden. Por eso aquí hay un 409 propio y no una
 * respuesta idempotente como la de ms-andesstay-mq: el cliente de Kafka no da la opción.
 *
 * Importa para quien integra: un alta repetida NO es inofensiva en esta API, hay que leer el
 * código de respuesta.
 */
public class RecursoYaExisteException extends RuntimeException {

    private final String nombre;

    public RecursoYaExisteException(String nombre, String detalleDelCluster) {
        super("el tópico '" + nombre + "' ya existe; Kafka no permite recrearlo, "
                + "y createTopics falla aunque la definición coincida"
                + (detalleDelCluster == null ? "" : " (" + detalleDelCluster + ")"));
        this.nombre = nombre;
    }

    public String getNombre() {
        return nombre;
    }
}
