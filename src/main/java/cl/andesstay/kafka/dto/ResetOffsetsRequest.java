package cl.andesstay.kafka.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.util.List;
import java.util.Locale;

/**
 * Petición de reposicionamiento de los offsets de un grupo.
 *
 * Las tres estrategias:
 *
 *   earliest    al offset más antiguo que quede en el log. El grupo reprocesa todo lo que la
 *               retención no se haya llevado
 *   latest      al final. El grupo se salta lo pendiente y sigue desde los mensajes nuevos
 *   explicito   a los offsets que indique la lista, partición por partición
 *
 * Qué particiones se mueven, con earliest y latest: las que el grupo tiene confirmadas, que son
 * las que aparecen en GET /consumer-groups/{id}. Con `topico` se limita a uno solo, que es lo
 * habitual cuando el grupo consume de varios.
 *
 * El grupo tiene que estar SIN miembros conectados. Kafka no permite mover los offsets de un
 * grupo activo, y la API responde 409 en lugar de dejar que el cluster conteste UnknownMemberId.
 */
public record ResetOffsetsRequest(

        @NotBlank(message = "la estrategia es obligatoria: earliest, latest o explicito")
        @Pattern(regexp = Restricciones.ESTRATEGIAS_DE_RESET,
                message = Restricciones.MENSAJE_ESTRATEGIAS)
        String estrategia,

        /** Limita el movimiento a un tópico. Si se omite, se mueven todos los del grupo. */
        @Size(max = Restricciones.LARGO_MAXIMO, message = Restricciones.MENSAJE_LARGO)
        @Pattern(regexp = Restricciones.CARACTERES_DE_NOMBRE,
                message = Restricciones.MENSAJE_CARACTERES_DE_NOMBRE)
        String topico,

        /**
         * Los offsets, solo para la estrategia explicito.
         *
         * La coherencia entre la estrategia y esta lista no se valida con anotaciones porque es
         * una regla entre dos campos: la comprueba ConsumerGroupService y sale como 400 con un
         * mensaje que explica cuál de las dos cosas falta o sobra.
         */
        @Valid List<OffsetDeParticion> offsets
) {

    public static final String EARLIEST = "earliest";
    public static final String LATEST = "latest";
    public static final String EXPLICITO = "explicito";

    /** La estrategia en minúsculas; la validación ya garantiza que es una de las tres. */
    public String estrategiaNormalizada() {
        return estrategia == null ? null : estrategia.toLowerCase(Locale.ROOT);
    }

    public boolean esExplicita() {
        return EXPLICITO.equals(estrategiaNormalizada());
    }

    public List<OffsetDeParticion> offsetsOVacios() {
        return offsets == null ? List.of() : offsets;
    }
}
