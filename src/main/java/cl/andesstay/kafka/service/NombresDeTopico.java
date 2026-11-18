package cl.andesstay.kafka.service;

/**
 * Validación del nombre antes de hablar con el cluster.
 *
 * Es el equivalente de NombresAmqp en ms-andesstay-mq, y existe por el mismo motivo: hay nombres
 * que el cluster acepta pero que no deberían crearse desde esta API, y otros que rechaza con un
 * error que no dice nada. Rechazarlos antes con un 400 explicado ahorra la vuelta.
 *
 * Los tres casos:
 *
 *   "." y ".."         Kafka los rechaza porque un tópico se almacena como un directorio con su
 *                      nombre, y esos dos ya significan algo en un sistema de archivos.
 *   prefijo "__"       es el que usa Kafka para sus propios tópicos (__consumer_offsets,
 *                      __transaction_state). Crear uno ahí no está prohibido por el cluster, pero
 *                      el listado de esta API lo escondería por interno, así que el tópico
 *                      quedaría creado e invisible.
 *   largo y caracteres el límite de 249 y el conjunto [A-Za-z0-9._-] son del propio Kafka.
 *
 * Hay un cuarto caso que NO se rechaza y conviene conocer: Kafka convierte tanto '.' como '_' en
 * '_' al formar los nombres de sus métricas, así que «reservations.events» y «reservations_events»
 * colisionan ahí. El cluster lo permite y solo deja un aviso en su log. Esta API no se mete:
 * prohibirlo dejaría fuera nombres legítimos por un detalle de las métricas.
 */
final class NombresDeTopico {

    /** Reservado por Kafka para sus tópicos internos. */
    private static final String PREFIJO_INTERNO = "__";

    /** Límite de Kafka para el nombre de un tópico. */
    private static final int LARGO_MAXIMO = 249;

    private NombresDeTopico() {
    }

    static void validar(String nombre) {
        if (nombre == null || nombre.isBlank()) {
            throw new IllegalArgumentException("el nombre del tópico es obligatorio");
        }
        if (".".equals(nombre) || "..".equals(nombre)) {
            throw new IllegalArgumentException(
                    "'.' y '..' no son nombres de tópico válidos: Kafka guarda cada tópico en un "
                            + "directorio con su nombre");
        }
        if (nombre.startsWith(PREFIJO_INTERNO)) {
            throw new IllegalArgumentException("el prefijo " + PREFIJO_INTERNO
                    + " está reservado por Kafka para sus tópicos internos; "
                    + "un tópico creado ahí quedaría oculto en el listado de esta API");
        }
        if (nombre.length() > LARGO_MAXIMO) {
            throw new IllegalArgumentException(
                    "el nombre no puede pasar de " + LARGO_MAXIMO + " caracteres");
        }
    }
}
