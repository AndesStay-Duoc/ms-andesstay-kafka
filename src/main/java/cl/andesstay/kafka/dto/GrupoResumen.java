package cl.andesstay.kafka.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Lo que la API devuelve de un grupo de consumidores en el listado.
 *
 * @param estado los estados de Kafka, tal cual los nombra el cluster: Stable es el grupo
 *               consumiendo con normalidad, Empty uno sin miembros pero con offsets guardados,
 *               PreparingRebalance y CompletingRebalance los dos momentos de un rebalanceo, y
 *               Dead un grupo que el cluster ya no conoce
 * @param simple true para los grupos que gestionan su asignación por su cuenta, sin el protocolo
 *               de pertenencia. No se rebalancean solos
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record GrupoResumen(
        String id,
        String estado,
        boolean simple
) {
}
