package cl.andesstay.kafka.dto;

import jakarta.validation.constraints.NotEmpty;

import java.util.Map;

/**
 * Petición de cambio de configuración de un tópico.
 *
 * Es un cambio INCREMENTAL: solo se tocan las claves que llegan, y las demás se quedan como
 * estaban. La alternativa de Kafka, alterConfigs a secas, reemplaza la configuración completa del
 * recurso, de modo que enviar solo retention.ms devolvería a su valor por omisión todo lo demás
 * que estuviera puesto. Esta API no la usa nunca.
 *
 * Un valor null quita la clave y devuelve esa configuración a su valor por omisión. Es
 * deliberado, y es la única forma de QUITAR una configuración en lugar de fijarla:
 *
 *   {"configuraciones": {"retention.ms": null}}
 */
public record ModificarConfiguracionRequest(

        @NotEmpty(message = "hay que enviar al menos una configuración")
        Map<String, String> configuraciones
) {
}
