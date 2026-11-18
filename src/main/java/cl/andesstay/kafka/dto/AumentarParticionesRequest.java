package cl.andesstay.kafka.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/**
 * Petición de aumento de particiones.
 *
 * El número es el TOTAL que tendrá el tópico, no cuántas se agregan. Es como lo define
 * createPartitions, y cambiar el significado aquí solo invitaría a equivocarse al comparar con el
 * log del cluster o con kafka-topics.
 */
public record AumentarParticionesRequest(

        @NotNull(message = "el total de particiones es obligatorio")
        @Min(value = 1, message = "tiene que ser al menos 1")
        @Max(value = Restricciones.MAXIMO_PARTICIONES,
                message = "esta API no pasa de " + Restricciones.MAXIMO_PARTICIONES + " particiones")
        Integer particiones
) {
}
