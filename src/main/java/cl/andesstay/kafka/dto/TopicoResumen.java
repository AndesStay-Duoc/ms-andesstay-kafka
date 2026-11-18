package cl.andesstay.kafka.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Lo que la API devuelve de un tópico en el listado.
 *
 * Es un resumen deliberado: el detalle partición por partición y las sesenta claves de
 * configuración están en GET /topics/{nombre}, porque en un listado no se leen.
 *
 * @param replicasEnSincronia  el MÍNIMO de réplicas al día entre todas las particiones del tópico
 * @param degradado            true si alguna partición tiene menos réplicas al día que réplicas
 *                             totales, o si alguna se quedó sin líder. Es la señal que importa:
 *                             con min.insync.replicas en 2, un tópico degradado deja de aceptar
 *                             publicaciones con acks=all aunque el cluster siga en pie
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record TopicoResumen(
        String nombre,
        boolean interno,
        boolean protegido,
        int particiones,
        int factorDeReplicacion,
        int replicasEnSincronia,
        boolean degradado
) {
}
