package cl.andesstay.kafka.dto;

/**
 * Formatos y rangos que la API acepta en nombres, particiones y offsets.
 *
 * Los dos primeros no son decisiones de esta API sino del propio Kafka: un nombre de tópico admite
 * hasta 249 caracteres del conjunto [A-Za-z0-9._-], y nada más. Tenerlos escritos aquí permite
 * rechazar con un 400 que señala el campo, en lugar de dejar que el cluster cierre la petición con
 * un InvalidTopic que hay que ir a buscar al log.
 */
public final class Restricciones {

    private Restricciones() {
    }

    /** Límite de Kafka para un nombre de tópico. No es una decisión de esta API. */
    public static final int LARGO_MAXIMO = 249;

    /**
     * '*' y no '+': el vacío ya lo informa @NotBlank, y así el mismo error no sale dos veces.
     *
     * El conjunto es exactamente el que admite Kafka. Es más estrecho que el de RabbitMQ —no
     * entran los dos puntos— así que la topología de este lado usa puntos como separador:
     * reservations.events, audit.timeline.
     */
    public static final String CARACTERES_DE_NOMBRE = "[A-Za-z0-9._-]*";
    public static final String MENSAJE_CARACTERES_DE_NOMBRE =
            "solo admite letras sin tilde, dígitos y los caracteres . _ -";

    /**
     * Tope de particiones por tópico que acepta esta API.
     *
     * Kafka no lo impone; lo impone el buen sentido. Cada partición son tres archivos abiertos por
     * réplica y un hueco más en cada rebalanceo del grupo, así que un dedo de más en el formulario
     * no debería poder pedir cien mil.
     */
    public static final int MAXIMO_PARTICIONES = 200;

    /** El cluster de AndesStay tiene tres brokers; más allá de diez no hay escenario razonable. */
    public static final int MAXIMO_REPLICACION = 10;

    /** Las tres formas de reposicionar un grupo. */
    public static final String ESTRATEGIAS_DE_RESET = "(?i)earliest|latest|explicito";
    public static final String MENSAJE_ESTRATEGIAS =
            "debe ser earliest, latest o explicito";

    public static final String MENSAJE_LARGO = "no puede pasar de " + LARGO_MAXIMO + " caracteres";
}
