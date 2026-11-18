package cl.andesstay.kafka.web;

/**
 * 404 · El tópico o el grupo de consumidores que se pidió no existe en el cluster.
 *
 * Es la misma decisión que tomó ms-andesstay-mq, y conviene mantenerla: operar sobre algo que no
 * existe es un error de quien llama, no un fallo del servicio, así que responde 404 y no 500. En
 * Kafka cuesta un poco más de trabajo que en RabbitMQ, porque varias operaciones del AdminClient
 * son tolerantes con lo inexistente: describeConsumerGroups devuelve un grupo en estado Dead en
 * lugar de fallar, y listConsumerGroupOffsets devuelve un mapa vacío. Ver ConsumerGroupService.
 */
public class RecursoNoEncontradoException extends RuntimeException {

    private final String recurso;
    private final String nombre;

    public RecursoNoEncontradoException(String recurso, String nombre) {
        super("no existe el " + recurso + " '" + nombre + "'");
        this.recurso = recurso;
        this.nombre = nombre;
    }

    public String getRecurso() {
        return recurso;
    }

    public String getNombre() {
        return nombre;
    }
}
