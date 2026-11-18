package cl.andesstay.kafka.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Respuesta de las operaciones de escritura.
 *
 * Existe para que un 201 o un 200 digan QUÉ pasó y no solo que no hubo error. El campo detalle es
 * el que lleva la información que la operación produjo: cuántas particiones tiene ahora el
 * tópico, qué claves se modificaron, cuántos offsets se movieron.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record OperacionResultado(
        String recurso,
        String nombre,
        String operacion,
        String detalle
) {

    public static OperacionResultado de(String recurso, String nombre, String operacion,
                                        String detalle) {
        return new OperacionResultado(recurso, nombre, operacion, detalle);
    }
}
