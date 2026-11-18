package cl.andesstay.kafka.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * El detalle de un grupo de consumidores: quién consume, por dónde va y cuánto le falta.
 *
 * Es la respuesta que resume de qué va este servicio. Reúne en una sola llamada lo que de otro
 * modo son tres operaciones distintas del AdminClient —describeConsumerGroups,
 * listConsumerGroupOffsets y listOffsets— más la resta que Kafka no hace por nadie: el lag.
 *
 * @param particiones         una fila por partición, con su offset confirmado, el final y el lag
 * @param distribucion        el mismo lag agrupado por consumidor, para ver si el reparto quedó
 *                            parejo
 * @param lagTotal            la suma de los lag de todas las particiones con offset confirmado
 * @param particionesSinConfirmar cuántas particiones no tienen ningún offset confirmado todavía.
 *                            No suman al lag total porque su retraso no está definido, pero
 *                            conviene saber que están
 * @param particionesSinAsignar cuántas particiones no las está leyendo ningún miembro. Si esto es
 *                            mayor que cero y el lag sube, ahí está el problema
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record GrupoDetalle(
        String id,
        String estado,
        boolean simple,
        NodoResumen coordinador,
        String estrategiaDeAsignacion,
        List<MiembroResumen> miembros,
        List<ParticionDeGrupo> particiones,
        List<ConsumoPorMiembro> distribucion,
        long lagTotal,
        int particionesSinConfirmar,
        int particionesSinAsignar
) {
}
