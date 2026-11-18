package cl.andesstay.kafka.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/** Un offset concreto para una partición concreta, en el reposicionamiento explícito. */
public record OffsetDeParticion(

        @NotBlank(message = "el tópico es obligatorio")
        @Size(max = Restricciones.LARGO_MAXIMO, message = Restricciones.MENSAJE_LARGO)
        @Pattern(regexp = Restricciones.CARACTERES_DE_NOMBRE,
                message = Restricciones.MENSAJE_CARACTERES_DE_NOMBRE)
        String topico,

        @NotNull(message = "la partición es obligatoria")
        @Min(value = 0, message = "las particiones se numeran desde 0")
        Integer particion,

        @NotNull(message = "el offset es obligatorio")
        @Min(value = 0, message = "un offset no puede ser negativo")
        Long offset
) {
}
