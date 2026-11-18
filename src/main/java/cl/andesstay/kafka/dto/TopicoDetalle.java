package cl.andesstay.kafka.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Map;

/**
 * El detalle de un tópico: cómo está repartido y con qué configuración corre.
 *
 * Las configuraciones se devuelven DOS veces y no es redundancia:
 *
 *   configuraciones             todas las claves efectivas, del orden de sesenta, con el valor que
 *                               el cluster aplica de verdad, vengan de donde vengan
 *   configuracionesPersonalizadas  solo las fijadas sobre este tópico, que son las que alguien
 *                               cambió a mano o por esta API
 *
 * La segunda es la que se mira para saber qué distingue a este tópico de los demás; la primera es
 * la que hace falta para responder «y entonces cuánto retiene».
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record TopicoDetalle(
        String nombre,
        boolean interno,
        boolean protegido,
        int particiones,
        int factorDeReplicacion,
        boolean degradado,
        List<ParticionDetalle> detalleDeParticiones,
        Map<String, String> configuraciones,
        Map<String, String> configuracionesPersonalizadas
) {
}
