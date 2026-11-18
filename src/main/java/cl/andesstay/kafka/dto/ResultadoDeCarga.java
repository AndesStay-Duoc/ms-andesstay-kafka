package cl.andesstay.kafka.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Lo que devuelve la prueba de carga: cuánto se publicó, en cuánto tiempo y a qué ritmo.
 *
 * El dato que interesa es mensajesPorSegundo, pero se devuelven también los crudos para que el
 * número se pueda comprobar y no haya que creérselo.
 *
 * @param fallidos mensajes que el cluster no confirmó. Con un tópico bien replicado tiene que ser
 *                 cero; si no lo es, el throughput de arriba no significa nada
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ResultadoDeCarga(
        String topico,
        int mensajes,
        int fallidos,
        long duracionMs,
        double mensajesPorSegundo
) {

    public static ResultadoDeCarga de(String topico, int mensajes, int fallidos, long duracionMs) {
        // Se redondea a dos decimales porque un double con quince cifras en un JSON de resultado
        // solo da trabajo de leer.
        double porSegundo = duracionMs <= 0
                ? mensajes
                : Math.round(mensajes * 1000.0 / duracionMs * 100.0) / 100.0;
        return new ResultadoDeCarga(topico, mensajes, fallidos, duracionMs, porSegundo);
    }
}
