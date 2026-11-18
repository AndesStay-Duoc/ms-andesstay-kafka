package cl.andesstay.kafka.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Cuánto le toca a cada consumidor del grupo. Es la distribución del consumo.
 *
 * Sirve para dos preguntas que no se responden mirando el lag total: si el reparto quedó parejo
 * entre los consumidores, y si hay uno que arrastra el retraso de todo el grupo. Un miembro con
 * tres particiones mientras otro tiene cero significa que el rebalanceo no repartió, y eso no se
 * arregla agregando consumidores: no hay más particiones que repartir.
 *
 * El grupo lleva también una fila con miembro null cuando hay particiones sin asignar, que es el
 * caso en el que el lag crece sin que nadie lo esté leyendo.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ConsumoPorMiembro(
        String miembro,
        String host,
        int particiones,
        long lag
) {
}
