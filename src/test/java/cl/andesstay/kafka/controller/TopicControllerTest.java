package cl.andesstay.kafka.controller;

import cl.andesstay.kafka.config.SecurityConfig;
import cl.andesstay.kafka.dto.OperacionResultado;
import cl.andesstay.kafka.dto.TopicoDetalle;
import cl.andesstay.kafka.service.TopicService;
import cl.andesstay.kafka.web.ApiDeGestionException;
import cl.andesstay.kafka.web.ManejadorDeErrores;
import cl.andesstay.kafka.web.RecursoNoEncontradoException;
import cl.andesstay.kafka.web.RecursoProtegidoException;
import cl.andesstay.kafka.web.RecursoYaExisteException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * La matriz de códigos HTTP del recurso de tópicos, de punta a punta de la capa web.
 *
 * Es la evidencia de autorización y de manejo de errores: cada aserción de aquí corresponde a una
 * fila de la batería de Postman, y se ejecuta sin cluster, sin tokens reales y sin tener que
 * levantar nada. Lo que NO cubre es la conversación con Kafka; eso es de TopicServiceTest y de
 * KafkaAdminClientTest.
 *
 * El JwtDecoder se simula porque si no el contexto intentaría descargar las claves públicas de
 * Entra ID al arrancar, y los tests dejarían de funcionar sin red.
 */
@WebMvcTest(TopicController.class)
@Import({SecurityConfig.class, ManejadorDeErrores.class})
class TopicControllerTest {

    private static final String RUTA = "/api/v1/kafka/topics";
    private static final String PROTEGIDO = "reservations.events";
    private static final String CUERPO_VALIDO = "{\"nombre\":\"pruebas.demo\"}";

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private TopicService servicio;

    @MockBean
    private JwtDecoder jwtDecoder;

    /** Token con el rol que la API exige. */
    private static MockHttpServletRequestBuilder comoAdmin(MockHttpServletRequestBuilder peticion) {
        return peticion.with(jwt().authorities(new SimpleGrantedAuthority("ROLE_Admin")));
    }

    /** Token válido pero del rol de huésped: autenticado y sin permiso. */
    private static MockHttpServletRequestBuilder comoCliente(MockHttpServletRequestBuilder peticion) {
        return peticion.with(jwt().authorities(new SimpleGrantedAuthority("ROLE_Cliente")));
    }

    // ── 401 · sin token ──────────────────────────────────────────────────────

    @Test
    @DisplayName("401 · sin token no se llega ni al controlador")
    void sinTokenEs401() throws Exception {
        mockMvc.perform(get(RUTA)).andExpect(status().isUnauthorized());
        mockMvc.perform(post(RUTA).contentType(MediaType.APPLICATION_JSON).content(CUERPO_VALIDO))
                .andExpect(status().isUnauthorized());
        mockMvc.perform(delete(RUTA + "/" + PROTEGIDO)).andExpect(status().isUnauthorized());

        // Lo esencial: la autorización corta ANTES del servicio. Un 401 que hubiera llegado a
        // tocar el cluster ya sería un problema.
        verify(servicio, never()).listar(org.mockito.ArgumentMatchers.anyBoolean());
        verify(servicio, never()).crear(any());
        verify(servicio, never()).eliminar(anyString());
    }

    // ── 403 · token válido, rol insuficiente ─────────────────────────────────

    @Test
    @DisplayName("403 · un token de Cliente está autenticado pero no administra tópicos")
    void clienteEs403() throws Exception {
        mockMvc.perform(comoCliente(get(RUTA)))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error").value("ROL_INSUFICIENTE"));
        mockMvc.perform(comoCliente(delete(RUTA + "/" + PROTEGIDO)))
                .andExpect(status().isForbidden());

        verify(servicio, never()).eliminar(anyString());
    }

    // ── 2xx · la ruta feliz ──────────────────────────────────────────────────

    @Test
    @DisplayName("201 · un Admin crea el tópico")
    void adminCrea201() throws Exception {
        when(servicio.crear(any())).thenReturn(
                OperacionResultado.de("tópico", "pruebas.demo", "creado", "creado con 3"));

        mockMvc.perform(comoAdmin(post(RUTA)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(CUERPO_VALIDO)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.nombre").value("pruebas.demo"))
                .andExpect(jsonPath("$.operacion").value("creado"));
    }

    @Test
    @DisplayName("200 · el listado esconde los internos salvo que se pidan")
    void listadoPasaElParametro() throws Exception {
        when(servicio.listar(org.mockito.ArgumentMatchers.anyBoolean())).thenReturn(List.of());

        mockMvc.perform(comoAdmin(get(RUTA))).andExpect(status().isOk());
        verify(servicio).listar(false);

        mockMvc.perform(comoAdmin(get(RUTA).param("incluirInternos", "true")))
                .andExpect(status().isOk());
        verify(servicio).listar(true);
    }

    @Test
    @DisplayName("200 · el detalle de un tópico con puntos en el nombre llega entero")
    void detalleConPuntosEnElNombre() throws Exception {
        // Importa afirmarlo: con el truncado de extensiones de versiones anteriores de Spring, un
        // nombre como audit.timeline.DLT habría llegado cortado al controlador.
        when(servicio.describir("audit.timeline.DLT")).thenReturn(new TopicoDetalle(
                "audit.timeline.DLT", false, true, 3, 3, false,
                List.of(), java.util.Map.of(), java.util.Map.of()));

        mockMvc.perform(comoAdmin(get(RUTA + "/audit.timeline.DLT")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.nombre").value("audit.timeline.DLT"))
                .andExpect(jsonPath("$.protegido").value(true));
    }

    @Test
    @DisplayName("200 · configurar un tópico responde con las claves que se tocaron")
    void configurar200() throws Exception {
        when(servicio.modificarConfiguracion(eq(PROTEGIDO), any())).thenReturn(
                OperacionResultado.de("tópico", PROTEGIDO, "configurado",
                        "se modificaron las claves [retention.ms]"));

        mockMvc.perform(comoAdmin(put(RUTA + "/" + PROTEGIDO + "/config")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"configuraciones\":{\"retention.ms\":\"604800000\"}}")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.operacion").value("configurado"));
    }

    @Test
    @DisplayName("200 · aumentar particiones")
    void particiones200() throws Exception {
        when(servicio.aumentarParticiones(eq("pruebas.demo"), any())).thenReturn(
                OperacionResultado.de("tópico", "pruebas.demo", "reparticionado",
                        "pasó de 3 a 6 particiones"));

        mockMvc.perform(comoAdmin(post(RUTA + "/pruebas.demo/particiones")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"particiones\":6}")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.detalle").value(
                        org.hamcrest.Matchers.containsString("de 3 a 6")));
    }

    @Test
    @DisplayName("204 · un Admin elimina un tópico libre, sin cuerpo en la respuesta")
    void adminElimina204() throws Exception {
        when(servicio.eliminar("pruebas.demo")).thenReturn(
                OperacionResultado.de("tópico", "pruebas.demo", "eliminado", "eliminado"));

        mockMvc.perform(comoAdmin(delete(RUTA + "/pruebas.demo")))
                .andExpect(status().isNoContent());
    }

    // ── 400 · peticiones mal formadas ────────────────────────────────────────

    @Test
    @DisplayName("400 · sin nombre, con el campo que falla señalado")
    void sinNombreEs400() throws Exception {
        mockMvc.perform(comoAdmin(post(RUTA)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("VALIDACION"))
                .andExpect(jsonPath("$.campos.nombre").exists());

        verify(servicio, never()).crear(any());
    }

    @Test
    @DisplayName("400 · un nombre con espacios, barras o tildes no llega al servicio")
    void nombreConCaracteresInvalidosEs400() throws Exception {
        mockMvc.perform(comoAdmin(post(RUTA)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"nombre\":\"reservas/año 1\"}")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.campos.nombre").value(
                        org.hamcrest.Matchers.containsString("solo admite")));

        verify(servicio, never()).crear(any());
    }

    @Test
    @DisplayName("400 · un factor de replicación fuera de rango se rechaza antes del cluster")
    void factorFueraDeRangoEs400() throws Exception {
        mockMvc.perform(comoAdmin(post(RUTA)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"nombre\":\"pruebas.demo\",\"factorDeReplicacion\":99}")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.campos.factorDeReplicacion").exists());

        verify(servicio, never()).crear(any());
    }

    @Test
    @DisplayName("400 · una configuración vacía no tiene nada que modificar")
    void configuracionVaciaEs400() throws Exception {
        mockMvc.perform(comoAdmin(put(RUTA + "/pruebas.demo/config")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"configuraciones\":{}}")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("VALIDACION"));
    }

    @Test
    @DisplayName("400 · reducir particiones, con la explicación de por qué Kafka no deja")
    void reducirParticionesEs400() throws Exception {
        when(servicio.aumentarParticiones(anyString(), any())).thenThrow(
                new IllegalArgumentException("Kafka no permite reducir las particiones"));

        mockMvc.perform(comoAdmin(post(RUTA + "/pruebas.demo/particiones")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"particiones\":1}")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("PETICION_INVALIDA"))
                .andExpect(jsonPath("$.detalle").value(
                        org.hamcrest.Matchers.containsString("no permite reducir")));
    }

    @Test
    @DisplayName("400 · JSON mal formado no sale como 500")
    void jsonRotoEs400() throws Exception {
        mockMvc.perform(comoAdmin(post(RUTA)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"nombre\":")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("CUERPO_ILEGIBLE"));
    }

    // ── 404 y 409 · los dos casos normales de una API de administración ──────

    @Test
    @DisplayName("404 · eliminar un tópico inexistente")
    void inexistenteEs404() throws Exception {
        when(servicio.eliminar(anyString()))
                .thenThrow(new RecursoNoEncontradoException("tópico", "no.existe"));

        mockMvc.perform(comoAdmin(delete(RUTA + "/no.existe")))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("RECURSO_NO_ENCONTRADO"))
                .andExpect(jsonPath("$.nombre").value("no.existe"));
    }

    @Test
    @DisplayName("409 · eliminar un tópico de la topología de AndesStay")
    void protegidoEs409() throws Exception {
        when(servicio.eliminar(PROTEGIDO))
                .thenThrow(new RecursoProtegidoException("tópico", PROTEGIDO, "eliminar"));

        mockMvc.perform(comoAdmin(delete(RUTA + "/" + PROTEGIDO)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("RECURSO_PROTEGIDO"))
                // El 409 tiene que decir dónde se decide la protección, o hay que leer el código
                // fuente.
                .andExpect(jsonPath("$.detalle").value(
                        org.hamcrest.Matchers.containsString("andesstay.kafka.protegidos")));
    }

    @Test
    @DisplayName("409 · crear un tópico que ya existe, que en Kafka no es idempotente")
    void yaExisteEs409() throws Exception {
        when(servicio.crear(any()))
                .thenThrow(new RecursoYaExisteException("pruebas.demo", "Topic already exists."));

        mockMvc.perform(comoAdmin(post(RUTA)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(CUERPO_VALIDO)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("TOPICO_YA_EXISTE"))
                .andExpect(jsonPath("$.nombre").value("pruebas.demo"));
    }

    // ── 5xx · el cluster no está o rechaza ───────────────────────────────────

    @Test
    @DisplayName("503 · el cluster no respondió dentro del límite")
    void clusterCaidoEs503() throws Exception {
        when(servicio.listar(org.mockito.ArgumentMatchers.anyBoolean()))
                .thenThrow(new ApiDeGestionException("venció el límite", false, null));

        mockMvc.perform(comoAdmin(get(RUTA)))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.error").value("CLUSTER_NO_DISPONIBLE"))
                .andExpect(jsonPath("$.detalle").value(
                        org.hamcrest.Matchers.containsString("andesstay.kafka.timeout-ms")));
    }

    @Test
    @DisplayName("502 · el cluster respondió, pero con un fallo del que no se sabe nada")
    void clusterRechazaEs502() throws Exception {
        when(servicio.listar(org.mockito.ArgumentMatchers.anyBoolean()))
                .thenThrow(new ApiDeGestionException("el cluster falló al listar", true, null));

        mockMvc.perform(comoAdmin(get(RUTA)))
                .andExpect(status().isBadGateway())
                .andExpect(jsonPath("$.error").value("CLUSTER_RECHAZO"));
    }

    @Test
    @DisplayName("503 · un timeout del cliente de Kafka que no pasó por KafkaAdminClient")
    void timeoutDeKafkaEs503() throws Exception {
        // Es la red de seguridad del manejador: sin ella, esto sería un 500 con traza.
        when(servicio.listar(org.mockito.ArgumentMatchers.anyBoolean()))
                .thenThrow(new org.apache.kafka.common.errors.TimeoutException("expired"));

        mockMvc.perform(comoAdmin(get(RUTA)))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.error").value("CLUSTER_NO_DISPONIBLE"));
    }

    // ── El health check, que es lo único abierto ─────────────────────────────

    @Test
    @DisplayName("el resto de Actuator no queda abierto junto al health")
    void actuatorNoQuedaAbierto() throws Exception {
        // /actuator/health es permitAll porque lo consulta el healthcheck del contenedor sin
        // token. Que esa excepción no se haya llevado por delante al resto conviene tenerlo
        // afirmado: /actuator/metrics describiría el cluster a cualquiera.
        mockMvc.perform(get("/actuator/metrics")).andExpect(status().isUnauthorized());
    }
}
