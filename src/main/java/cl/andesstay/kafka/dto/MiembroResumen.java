package cl.andesstay.kafka.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * Un consumidor conectado al grupo.
 *
 * @param id         el consumerId que asignó el coordinador. Cambia en cada reconexión
 * @param cliente    el client.id que configuró la aplicación. Es el que identifica al servicio
 * @param host       desde dónde se conectó
 * @param asignacion sus particiones, en el formato que usan Kafka y su consola:
 *                   «reservations.events-0»
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record MiembroResumen(
        String id,
        String cliente,
        String host,
        List<String> asignacion
) {
}
