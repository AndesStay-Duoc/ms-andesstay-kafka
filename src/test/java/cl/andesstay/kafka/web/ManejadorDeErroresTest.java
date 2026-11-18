package cl.andesstay.kafka.web;

import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * La traducción de los errores propios a códigos HTTP, sin levantar la capa web.
 *
 * Los controladores ya prueban que el manejador está enganchado; lo que se mira aquí es la
 * decisión de cada caso, que es donde un cambio inocente deja de distinguir «el cluster no está»
 * de «el cluster rechazó», y quien integra pierde la única señal que tenía para saber si
 * reintentar.
 */
class ManejadorDeErroresTest {

    private final ManejadorDeErrores manejador = new ManejadorDeErrores();

    private static String error(ResponseEntity<Map<String, Object>> respuesta) {
        assertNotNull(respuesta.getBody());
        return String.valueOf(respuesta.getBody().get("error"));
    }

    private static String detalle(ResponseEntity<Map<String, Object>> respuesta) {
        assertNotNull(respuesta.getBody());
        return String.valueOf(respuesta.getBody().get("detalle"));
    }

    // ── El cluster ───────────────────────────────────────────────────────────

    @Test
    @DisplayName("503 · sin respuesta del cluster, y el mensaje dice dónde mirar")
    void clusterInalcanzableEs503() {
        ResponseEntity<Map<String, Object>> respuesta = manejador.cluster(
                new ApiDeGestionException("venció el límite al listar los tópicos", false, null));

        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, respuesta.getStatusCode());
        assertEquals("CLUSTER_NO_DISPONIBLE", error(respuesta));
        // Un 503 que no dice qué comprobar obliga a leer el código fuente del servicio.
        assertTrue(detalle(respuesta).contains("andesstay.kafka.timeout-ms"), detalle(respuesta));
        assertTrue(detalle(respuesta).contains("bootstrap-servers"), detalle(respuesta));
    }

    @Test
    @DisplayName("502 · el cluster contestó, pero rechazando")
    void clusterRechazaEs502() {
        ResponseEntity<Map<String, Object>> respuesta = manejador.cluster(
                new ApiDeGestionException("el cluster falló al crear el tópico", true, null));

        // La diferencia con el 503 importa: un 502 no se arregla esperando a que el cluster
        // vuelva, porque el cluster está.
        assertEquals(HttpStatus.BAD_GATEWAY, respuesta.getStatusCode());
        assertEquals("CLUSTER_RECHAZO", error(respuesta));
    }

    @Test
    @DisplayName("503 · un timeout del cliente de Kafka que no pasó por KafkaAdminClient")
    void timeoutDeKafkaEs503() {
        ResponseEntity<Map<String, Object>> respuesta =
                manejador.errorDeKafka(new TimeoutException("expired"));

        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, respuesta.getStatusCode());
        assertEquals("CLUSTER_NO_DISPONIBLE", error(respuesta));
    }

    @Test
    @DisplayName("502 · cualquier otro error del cliente de Kafka")
    void otroErrorDeKafkaEs502() {
        ResponseEntity<Map<String, Object>> respuesta = manejador.errorDeKafka(
                new org.apache.kafka.common.KafkaException("serializador roto"));

        assertEquals(HttpStatus.BAD_GATEWAY, respuesta.getStatusCode());
        assertEquals("ERROR_DEL_CLUSTER", error(respuesta));
        assertNotNull(respuesta.getBody().get("detalleDelCluster"));
    }

    // ── Los errores de negocio ───────────────────────────────────────────────

    @Test
    @DisplayName("404 · el cuerpo nombra el recurso y el nombre, no solo el texto")
    void noEncontradoLlevaElRecurso() {
        ResponseEntity<Map<String, Object>> respuesta = manejador.noEncontrado(
                new RecursoNoEncontradoException("tópico", "no.existe"));

        assertEquals(HttpStatus.NOT_FOUND, respuesta.getStatusCode());
        // Los campos permiten reaccionar sin tener que leer el texto del detalle.
        assertEquals("tópico", respuesta.getBody().get("recurso"));
        assertEquals("no.existe", respuesta.getBody().get("nombre"));
    }

    @Test
    @DisplayName("409 · el tópico protegido, con el sitio donde se decide la protección")
    void protegidoEs409() {
        ResponseEntity<Map<String, Object>> respuesta = manejador.protegido(
                new RecursoProtegidoException("tópico", "reservations.events", "eliminar"));

        assertEquals(HttpStatus.CONFLICT, respuesta.getStatusCode());
        assertEquals("RECURSO_PROTEGIDO", error(respuesta));
        assertTrue(detalle(respuesta).contains("andesstay.kafka.protegidos"), detalle(respuesta));
        assertTrue(detalle(respuesta).contains("irreversible"), detalle(respuesta));
    }

    @Test
    @DisplayName("409 · el tópico que ya existe, que no es un alta repetida inofensiva")
    void yaExisteEs409() {
        ResponseEntity<Map<String, Object>> respuesta = manejador.yaExiste(
                new RecursoYaExisteException("pruebas.demo", "Topic already exists."));

        assertEquals(HttpStatus.CONFLICT, respuesta.getStatusCode());
        assertEquals("TOPICO_YA_EXISTE", error(respuesta));
        assertEquals("pruebas.demo", respuesta.getBody().get("nombre"));
    }

    @Test
    @DisplayName("409 · el grupo activo, con el estado y los miembros que lo impiden")
    void grupoActivoEs409() {
        ResponseEntity<Map<String, Object>> respuesta = manejador.grupoActivo(
                new GrupoActivoException("andesstay-audit", "Stable",
                        List.of("audit-1", "audit-2"), "mover los offsets de"));

        assertEquals(HttpStatus.CONFLICT, respuesta.getStatusCode());
        assertEquals("GRUPO_ACTIVO", error(respuesta));
        assertEquals("Stable", respuesta.getBody().get("estado"));
        assertEquals(List.of("audit-1", "audit-2"), respuesta.getBody().get("miembros"));
        // Un 409 sin salida es un callejón: el detalle tiene que decir qué hacer.
        assertTrue(detalle(respuesta).contains("detener sus consumidores"), detalle(respuesta));
    }

    @Test
    @DisplayName("400 · una petición inválida conserva el mensaje que explica el motivo")
    void invalidaEs400() {
        ResponseEntity<Map<String, Object>> respuesta = manejador.peticionInvalida(
                new IllegalArgumentException("Kafka no permite reducir las particiones"));

        assertEquals(HttpStatus.BAD_REQUEST, respuesta.getStatusCode());
        assertEquals("PETICION_INVALIDA", error(respuesta));
        assertTrue(detalle(respuesta).contains("no permite reducir"), detalle(respuesta));
    }

    @Test
    @DisplayName("403 · sin el rol Admin, y nunca un 500")
    void sinRolEs403() {
        ResponseEntity<Map<String, Object>> respuesta =
                manejador.sinPermiso(new AccessDeniedException("nope"));

        assertEquals(HttpStatus.FORBIDDEN, respuesta.getStatusCode());
        assertEquals("ROL_INSUFICIENTE", error(respuesta));
    }
}
