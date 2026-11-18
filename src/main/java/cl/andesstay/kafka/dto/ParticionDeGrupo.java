package cl.andesstay.kafka.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * El avance de un grupo sobre una partición, que es donde se ve el lag.
 *
 * @param offsetConfirmado el último offset que el grupo confirmó. Viene en null cuando el grupo
 *                         tiene la partición asignada pero todavía no ha confirmado nada en ella
 * @param offsetFinal      el offset que tendrá el próximo mensaje que se publique
 * @param lag              offsetFinal menos offsetConfirmado: cuántos mensajes quedan por
 *                         consumir. Null si no hay offset confirmado, porque entonces el retraso
 *                         no está definido: depende de dónde empiece a leer el consumidor
 * @param miembro          el client.id del consumidor que tiene asignada la partición, o null si
 *                         ninguno la tiene. Una partición con lag y sin miembro es el caso que
 *                         importa ver: nadie la está leyendo
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ParticionDeGrupo(
        String topico,
        int particion,
        Long offsetConfirmado,
        Long offsetFinal,
        Long lag,
        String miembro
) {
}
