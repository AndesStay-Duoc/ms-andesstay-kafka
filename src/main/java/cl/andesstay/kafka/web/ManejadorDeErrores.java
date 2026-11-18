package cl.andesstay.kafka.web;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.KafkaException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Traduce los errores del cluster a códigos HTTP que signifiquen algo para quien llama.
 *
 * Es lo que separa una API de administración usable de una que devuelve 500 en sus casos más
 * normales. Los cuatro que aparecen a diario administrando Kafka son:
 *
 *   crear un tópico que ya existe            -> TopicExists        -> 409
 *   operar sobre algo que no existe          -> UnknownTopic...    -> 404
 *   mover offsets de un grupo con miembros   -> UnknownMemberId    -> 409
 *   el cluster no responde                   -> Timeout            -> 503
 *
 * La traducción desde los tipos de org.apache.kafka la hace broker/KafkaAdminClient, que es el
 * único sitio del servicio que los conoce; aquí solo llegan ya las excepciones propias. El
 * handler de KafkaException que hay más abajo es la red de seguridad para lo que no pasa por ese
 * cliente, que es el productor de la prueba de carga.
 */
@Slf4j
@RestControllerAdvice
public class ManejadorDeErrores {

    // ── Errores de negocio ───────────────────────────────────────────────

    /** 404 · No existe el tópico o el grupo. */
    @ExceptionHandler(RecursoNoEncontradoException.class)
    public ResponseEntity<Map<String, Object>> noEncontrado(RecursoNoEncontradoException e) {
        return cuerpo(HttpStatus.NOT_FOUND, "RECURSO_NO_ENCONTRADO", e.getMessage(),
                Map.of("recurso", e.getRecurso(), "nombre", e.getNombre()));
    }

    /** 409 · Es un tópico de la topología de AndesStay y esta API no lo elimina. */
    @ExceptionHandler(RecursoProtegidoException.class)
    public ResponseEntity<Map<String, Object>> protegido(RecursoProtegidoException e) {
        log.warn("[Protección] Se rechazó una operación sobre el recurso protegido {}: {}",
                e.getNombre(), e.getMessage());
        return cuerpo(HttpStatus.CONFLICT, "RECURSO_PROTEGIDO", e.getMessage(),
                Map.of("nombre", e.getNombre()));
    }

    /**
     * 409 · El tópico ya existe.
     *
     * No es un caso idempotente como la redeclaración de una cola en AMQP: createTopics falla
     * aunque las particiones y la configuración coincidan. Ver RecursoYaExisteException.
     */
    @ExceptionHandler(RecursoYaExisteException.class)
    public ResponseEntity<Map<String, Object>> yaExiste(RecursoYaExisteException e) {
        return cuerpo(HttpStatus.CONFLICT, "TOPICO_YA_EXISTE", e.getMessage(),
                Map.of("nombre", e.getNombre()));
    }

    /** 409 · El grupo tiene miembros conectados y Kafka no deja tocarle los offsets. */
    @ExceptionHandler(GrupoActivoException.class)
    public ResponseEntity<Map<String, Object>> grupoActivo(GrupoActivoException e) {
        log.warn("[Grupos] Se rechazó una operación sobre el grupo activo {} ({} miembros)",
                e.getGrupo(), e.getMiembros().size());
        return cuerpo(HttpStatus.CONFLICT, "GRUPO_ACTIVO", e.getMessage(),
                Map.of("grupo", e.getGrupo(), "estado", e.getEstado(), "miembros", e.getMiembros()));
    }

    /** 400 · Nombre reservado, particiones que no suben, rango fuera de lo admitido. */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> peticionInvalida(IllegalArgumentException e) {
        return cuerpo(HttpStatus.BAD_REQUEST, "PETICION_INVALIDA", e.getMessage(), Map.of());
    }

    /** 400 · Falló la validación del cuerpo. Se devuelve campo por campo. */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> validacion(MethodArgumentNotValidException e) {
        Map<String, Object> campos = new LinkedHashMap<>();
        e.getBindingResult().getFieldErrors()
                .forEach(f -> campos.put(f.getField(), f.getDefaultMessage()));
        return cuerpo(HttpStatus.BAD_REQUEST, "VALIDACION", "el cuerpo no es válido",
                Map.of("campos", campos));
    }

    /**
     * 400 · Falló la validación de un parámetro de la URL, como el count de la prueba de carga.
     *
     * Es el equivalente de validacion() para @RequestParam: Spring 6.1 valida esas anotaciones por
     * su cuenta y lanza esta excepción en lugar de MethodArgumentNotValidException. Se responde con
     * el mismo código y la misma forma, para que quien llama no tenga que distinguir dónde iba el
     * valor.
     */
    @ExceptionHandler(HandlerMethodValidationException.class)
    public ResponseEntity<Map<String, Object>> validacionDeParametros(HandlerMethodValidationException e) {
        Map<String, Object> campos = new LinkedHashMap<>();
        e.getAllValidationResults().forEach(r -> campos.put(
                r.getMethodParameter().getParameterName(),
                r.getResolvableErrors().get(0).getDefaultMessage()));
        return cuerpo(HttpStatus.BAD_REQUEST, "VALIDACION", "los parámetros no son válidos",
                Map.of("campos", campos));
    }

    /** 400 · Un parámetro de la URL que no se puede convertir, como ?count=muchos. */
    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<Map<String, Object>> tipoDeParametro(MethodArgumentTypeMismatchException e) {
        return cuerpo(HttpStatus.BAD_REQUEST, "PETICION_INVALIDA",
                "el parámetro '" + e.getName() + "' no admite el valor '" + e.getValue() + "'",
                Map.of());
    }

    /**
     * 400 · JSON mal formado, o un valor que no encaja en el tipo del campo.
     *
     * Se trata aparte de la validación porque ocurre ANTES: Jackson falla al construir el objeto,
     * así que las anotaciones de jakarta.validation nunca llegan a evaluarse.
     */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, Object>> cuerpoIlegible(HttpMessageNotReadableException e) {
        return cuerpo(HttpStatus.BAD_REQUEST, "CUERPO_ILEGIBLE",
                "el cuerpo no se pudo interpretar; revisa el JSON",
                Map.of("detalle", String.valueOf(e.getMostSpecificCause().getMessage())));
    }

    /** 403 · Autenticado pero sin el rol Admin. Nunca 500. */
    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<Map<String, Object>> sinPermiso(AccessDeniedException e) {
        return cuerpo(HttpStatus.FORBIDDEN, "ROL_INSUFICIENTE",
                "esta operación exige el rol Admin", Map.of());
    }

    // ── Errores del cluster ──────────────────────────────────────────────

    /** 503 o 502 · según si el cluster llegó a responder algo o no respondió en absoluto. */
    @ExceptionHandler(ApiDeGestionException.class)
    public ResponseEntity<Map<String, Object>> cluster(ApiDeGestionException e) {
        if (!e.esAlcanzable()) {
            log.error("[Cluster] Sin respuesta del cluster: {}", e.getMessage());
            return cuerpo(HttpStatus.SERVICE_UNAVAILABLE, "CLUSTER_NO_DISPONIBLE",
                    "no hubo respuesta del cluster de Kafka dentro del tiempo configurado "
                            + "(andesstay.kafka.timeout-ms). Comprueba que los brokers estén en "
                            + "marcha y que andesstay.kafka.bootstrap-servers sea alcanzable.",
                    Map.of());
        }
        log.error("[Cluster] El cluster rechazó la operación: {}", e.getMessage());
        return cuerpo(HttpStatus.BAD_GATEWAY, "CLUSTER_RECHAZO", e.getMessage(), Map.of());
    }

    /**
     * La red de seguridad para lo que no pasa por KafkaAdminClient.
     *
     * Hoy es solo el productor de la prueba de carga, que habla con el cluster por su cuenta a
     * través del KafkaTemplate. Sin este handler, un cluster caído durante la demostración
     * saldría como un 500 con traza en lugar de un 503 que se explica solo.
     */
    @ExceptionHandler(KafkaException.class)
    public ResponseEntity<Map<String, Object>> errorDeKafka(KafkaException e) {
        boolean vencio = e instanceof org.apache.kafka.common.errors.TimeoutException;
        log.error("[Cluster] Error del cliente de Kafka ({})", e.getClass().getSimpleName(), e);
        return vencio
                ? cuerpo(HttpStatus.SERVICE_UNAVAILABLE, "CLUSTER_NO_DISPONIBLE",
                        "la operación venció sin respuesta del cluster",
                        Map.of("detalleDelCluster", String.valueOf(e.getMessage())))
                : cuerpo(HttpStatus.BAD_GATEWAY, "ERROR_DEL_CLUSTER",
                        "el cluster rechazó la operación",
                        Map.of("detalleDelCluster", String.valueOf(e.getMessage())));
    }

    // ── Apoyo ────────────────────────────────────────────────────────────

    /**
     * No se captura Exception a secas a propósito: un fallo inesperado tiene que seguir siendo un
     * 500 con su traza en el log. Convertir todo en 4xx escondería defectos reales.
     */
    private static ResponseEntity<Map<String, Object>> cuerpo(
            HttpStatus estado, String error, String detalle, Map<String, Object> extra) {
        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("error", error);
        respuesta.put("detalle", detalle);
        respuesta.putAll(extra);
        return ResponseEntity.status(estado).body(respuesta);
    }
}
