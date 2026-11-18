package cl.andesstay.kafka.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * El reparto de una partición entre los brokers del cluster.
 *
 * @param lider      el broker que atiende lecturas y escrituras de esta partición. Viene en null
 *                   mientras no hay ninguno elegido, que es lo que se ve con una partición sin
 *                   servicio: ahí ni se publica ni se consume
 * @param replicas   los brokers que guardan una copia
 * @param isr        las réplicas al día (in-sync replicas). Que sean menos que las réplicas es la
 *                   señal de que el tópico está degradado
 * @param enSincronia true cuando todas las réplicas están al día
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ParticionDetalle(
        int particion,
        NodoResumen lider,
        List<NodoResumen> replicas,
        List<NodoResumen> isr,
        boolean enSincronia
) {
}
