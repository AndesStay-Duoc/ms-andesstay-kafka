package cl.andesstay.kafka.web;

/**
 * 409 · Se intentó eliminar un tópico de la topología de AndesStay.
 *
 * Eliminar un tópico en Kafka es irreversible: se va el log de todas sus particiones en los tres
 * brokers y, con él, los offsets confirmados de los grupos que lo consumían. Un DELETE sobre
 * reservations.events corta el flujo de eventos hacia ms-andesstay-audit y no deja nada que
 * restaurar.
 *
 * La protección no se puede saltar con un parámetro de la petición. Cambiarla exige cambiar la
 * lista andesstay.kafka.protegidos y volver a desplegar, que es justamente el punto: un seguro que
 * se desactiva con un parámetro de la misma petición protege del error de dedo, pero no de un
 * script equivocado ni de quien no conoce la topología.
 *
 * Alcanza solo al borrado. Cambiar una configuración de un tópico protegido sí se permite: es
 * reversible, y es justo lo que la evaluación pide poder demostrar sobre un tópico real.
 */
public class RecursoProtegidoException extends RuntimeException {

    private final String nombre;

    /** Operación sobre el propio recurso protegido; por ejemplo «eliminar». */
    public RecursoProtegidoException(String recurso, String nombre, String operacion) {
        this(nombre, operacion + " el " + recurso + " '" + nombre + "'");
    }

    /**
     * @param nombre    el recurso protegido que motiva el rechazo
     * @param operacion lo que se intentó, ya redactado
     */
    public RecursoProtegidoException(String nombre, String operacion) {
        super("No se permite " + operacion + ": forma parte de la topología de AndesStay "
                + "(andesstay.kafka.protegidos) y esta API no la elimina. "
                + "Eliminar un tópico en Kafka es irreversible.");
        this.nombre = nombre;
    }

    public String getNombre() {
        return nombre;
    }
}
