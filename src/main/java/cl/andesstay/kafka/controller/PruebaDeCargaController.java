package cl.andesstay.kafka.controller;

import cl.andesstay.kafka.ApiPaths;
import cl.andesstay.kafka.config.KafkaProperties;
import cl.andesstay.kafka.dto.ResultadoDeCarga;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Prueba de carga: publica N mensajes y mide cuántos por segundo entraron.
 *
 * PARA QUÉ ESTÁ. Para generar lag visible. Un grupo de consumidores sin retraso no demuestra
 * nada: el lag, la distribución del consumo y el efecto de agregar consumidores solo se ven
 * cuando hay mensajes pendientes de verdad. Diez mil mensajes sobre un tópico con un consumidor
 * lento bastan para que GET /consumer-groups/{id} y Kafka UI muestren un retraso que baja mientras
 * se mira.
 *
 * POR QUÉ ESTÁ DETRÁS DE UN PERFIL. Porque es lo contrario de administrar: un administrador que
 * puede inyectar diez mil mensajes en un tópico de producción no es un administrador. Sin
 * `--spring.profiles.active=demo` esta clase no se registra y la ruta no existe: devuelve 404, no
 * 403, porque no hay nada ahí.
 *
 * El productor y sus tres parámetros de la guía 3.3.2 del curso están en
 * config/ProductorDeCargaConfig.
 *
 * NO lleva @Validated a propósito, aunque el parámetro count tenga restricciones. Desde Spring
 * 6.1 los controladores validan sus @RequestParam por su cuenta y lanzan
 * HandlerMethodValidationException, que ManejadorDeErrores traduce a un 400 con el campo
 * señalado. Con @Validated se activa en su lugar la validación por proxy AOP, que lanza una
 * ConstraintViolationException sin manejador y acaba en un 500.
 */
@Slf4j
@RestController
@RequestMapping(ApiPaths.PRUEBAS)
@PreAuthorize("hasRole('Admin')")
@Profile("demo")
public class PruebaDeCargaController {

    /** Tope por llamada. Un millón de mensajes es más de lo que una demostración necesita. */
    private static final int MAXIMO_MENSAJES = 1_000_000;

    private final KafkaTemplate<String, String> plantilla;
    private final KafkaProperties propiedades;

    public PruebaDeCargaController(KafkaTemplate<String, String> plantilla,
                                   KafkaProperties propiedades) {
        this.plantilla = plantilla;
        this.propiedades = propiedades;
    }

    /**
     * 200 · Publica los mensajes y devuelve el throughput.
     *
     * El tópico tiene que existir: el cluster del caso corre con auto.create.topics.enable=false,
     * así que publicar sobre un nombre inexistente no lo crea, falla. Se crea antes con
     * POST /api/v1/kafka/topics.
     *
     * CÓMO SE MIDE. Se encolan los N envíos, se fuerza el vaciado del lote pendiente y se espera
     * la confirmación de todos antes de parar el cronómetro. Medir solo hasta el send() daría un
     * número bonito y falso: send() no publica, encola. Con acks=all, lo que se mide es el tiempo
     * hasta que las réplicas en sincronía confirmaron, que es el rendimiento que el sistema tiene
     * de verdad.
     *
     * @param count  cuántos mensajes publicar
     * @param topico sobre cuál. Por omisión, andesstay.kafka.pruebas.topico
     */
    @PostMapping("/carga")
    public ResultadoDeCarga generarCarga(
            @RequestParam(defaultValue = "10000")
            @Min(value = 1, message = "hay que publicar al menos 1 mensaje")
            @Max(value = MAXIMO_MENSAJES,
                    message = "esta prueba no publica más de " + MAXIMO_MENSAJES + " mensajes")
            int count,
            @RequestParam(required = false) String topico) {

        String destino = topico != null && !topico.isBlank()
                ? topico
                : propiedades.pruebas().topico();

        log.warn("[Carga] Publicando {} mensajes en {}", count, destino);
        long inicio = System.nanoTime();

        List<CompletableFuture<SendResult<String, String>>> envios = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            // La clave reparte los mensajes entre las particiones del tópico: con una sola clave
            // caerían todos en la misma y el lag se concentraría en un consumidor, que es
            // justamente lo que la demostración no quiere mostrar.
            envios.add(plantilla.send(destino, "carga-" + i, mensaje(i)));
        }
        // send() encola; sin esto, el último lote se quedaría esperando sus linger.ms.
        plantilla.flush();

        int fallidos = 0;
        for (CompletableFuture<SendResult<String, String>> envio : envios) {
            try {
                envio.join();
            } catch (CompletionException e) {
                fallidos++;
            }
        }

        long duracionMs = (System.nanoTime() - inicio) / 1_000_000;
        ResultadoDeCarga resultado = ResultadoDeCarga.de(destino, count, fallidos, duracionMs);
        log.warn("[Carga] {} mensajes en {} ms = {} msg/s ({} fallidos)",
                count, duracionMs, resultado.mensajesPorSegundo(), fallidos);
        return resultado;
    }

    /**
     * Un mensaje de tamaño parecido al de un evento de reserva.
     *
     * Importa que no sea una cadena de tres letras: con mensajes diminutos, el lote de 32 KB no se
     * llena nunca y la prueba mide la latencia de linger.ms en lugar del rendimiento.
     */
    private static String mensaje(int indice) {
        return "{\"indice\":" + indice
                + ",\"origen\":\"ms-andesstay-kafka\""
                + ",\"instante\":\"" + java.time.Instant.now() + "\""
                + ",\"relleno\":\"carga de prueba para la supervisión de lag de AndesStay\"}";
    }
}
