package cl.andesstay.kafka.controller;

import cl.andesstay.kafka.config.SecurityConfig;
import cl.andesstay.kafka.dto.ConsumoPorMiembro;
import cl.andesstay.kafka.dto.GrupoDetalle;
import cl.andesstay.kafka.dto.GrupoResumen;
import cl.andesstay.kafka.dto.MiembroResumen;
import cl.andesstay.kafka.dto.NodoResumen;
import cl.andesstay.kafka.dto.OperacionResultado;
import cl.andesstay.kafka.dto.ParticionDeGrupo;
import cl.andesstay.kafka.service.ConsumerGroupService;
import cl.andesstay.kafka.web.GrupoActivoException;
import cl.andesstay.kafka.web.ManejadorDeErrores;
import cl.andesstay.kafka.web.RecursoNoEncontradoException;
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
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * La matriz de códigos HTTP del recurso de grupos, y la forma de la respuesta del lag.
 *
 * La forma importa tanto como el código: GET /consumer-groups/{id} es la consulta que se enseña
 * en la evaluación, así que aquí queda afirmado que el JSON trae el lag por partición, el total y
 * la distribución por consumidor con los nombres que el README documenta. Un cambio de nombre de
 * campo rompe este test antes que la demostración.
 */
@WebMvcTest(ConsumerGroupController.class)
@Import({SecurityConfig.class, ManejadorDeErrores.class})
class ConsumerGroupControllerTest {

    private static final String RUTA = "/api/v1/kafka/consumer-groups";
    private static final String GRUPO = "andesstay-audit";
    private static final String TOPICO = "reservations.events";

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private ConsumerGroupService servicio;

    @MockBean
    private JwtDecoder jwtDecoder;

    private static MockHttpServletRequestBuilder comoAdmin(MockHttpServletRequestBuilder peticion) {
        return peticion.with(jwt().authorities(new SimpleGrantedAuthority("ROLE_Admin")));
    }

    private static MockHttpServletRequestBuilder comoCliente(MockHttpServletRequestBuilder peticion) {
        return peticion.with(jwt().authorities(new SimpleGrantedAuthority("ROLE_Cliente")));
    }

    /** Un grupo con dos consumidores, una partición huérfana y lag repartido. */
    private static GrupoDetalle detalleDeEjemplo() {
        return new GrupoDetalle(
                GRUPO, "Stable", false,
                new NodoResumen(1, "kafka1", 29092),
                "range",
                List.of(new MiembroResumen("consumidor-1", "audit-1", "10.0.0.5",
                        List.of(TOPICO + "-0"))),
                List.of(
                        new ParticionDeGrupo(TOPICO, 0, 100L, 150L, 50L, "audit-1"),
                        new ParticionDeGrupo(TOPICO, 1, null, 800L, null, "audit-1"),
                        new ParticionDeGrupo(TOPICO, 2, 300L, 1000L, 700L, null)),
                List.of(
                        new ConsumoPorMiembro("audit-1", "10.0.0.5", 2, 50L),
                        new ConsumoPorMiembro(null, null, 1, 700L)),
                750L, 1, 1);
    }

    // ── Autorización ─────────────────────────────────────────────────────────

    @Test
    @DisplayName("401 · sin token no se llega ni al controlador")
    void sinTokenEs401() throws Exception {
        mockMvc.perform(get(RUTA)).andExpect(status().isUnauthorized());
        mockMvc.perform(get(RUTA + "/" + GRUPO)).andExpect(status().isUnauthorized());
        mockMvc.perform(delete(RUTA + "/" + GRUPO)).andExpect(status().isUnauthorized());

        verify(servicio, never()).describir(anyString());
        verify(servicio, never()).eliminar(anyString());
    }

    @Test
    @DisplayName("403 · un token de Cliente no mueve los offsets de nadie")
    void clienteEs403() throws Exception {
        mockMvc.perform(comoCliente(post(RUTA + "/" + GRUPO + "/offsets/reset")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"estrategia\":\"latest\"}")))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error").value("ROL_INSUFICIENTE"));

        verify(servicio, never()).reposicionar(anyString(), any());
    }

    // ── La consulta del lag ──────────────────────────────────────────────────

    @Test
    @DisplayName("200 · el detalle trae el lag por partición, el total y la distribución")
    void detalleTraeElLag() throws Exception {
        when(servicio.describir(GRUPO)).thenReturn(detalleDeEjemplo());

        mockMvc.perform(comoAdmin(get(RUTA + "/" + GRUPO)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(GRUPO))
                .andExpect(jsonPath("$.estado").value("Stable"))
                .andExpect(jsonPath("$.lagTotal").value(750))
                // Partición por partición, con el offset confirmado y el final.
                .andExpect(jsonPath("$.particiones[0].topico").value(TOPICO))
                .andExpect(jsonPath("$.particiones[0].offsetConfirmado").value(100))
                .andExpect(jsonPath("$.particiones[0].offsetFinal").value(150))
                .andExpect(jsonPath("$.particiones[0].lag").value(50))
                .andExpect(jsonPath("$.particiones[0].miembro").value("audit-1"))
                // La distribución del consumo, que es lo que dice si el reparto quedó parejo.
                .andExpect(jsonPath("$.distribucion[0].miembro").value("audit-1"))
                .andExpect(jsonPath("$.distribucion[0].particiones").value(2))
                .andExpect(jsonPath("$.distribucion[0].lag").value(50))
                .andExpect(jsonPath("$.particionesSinConfirmar").value(1))
                .andExpect(jsonPath("$.particionesSinAsignar").value(1));
    }

    @Test
    @DisplayName("una partición sin offset confirmado no informa un lag inventado")
    void sinConfirmarNoInventaLag() throws Exception {
        when(servicio.describir(GRUPO)).thenReturn(detalleDeEjemplo());

        mockMvc.perform(comoAdmin(get(RUTA + "/" + GRUPO)))
                .andExpect(status().isOk())
                // Los campos nulos no se serializan: el consumidor de la API ve que no están, que
                // es distinto de ver un cero.
                .andExpect(jsonPath("$.particiones[1].lag").doesNotExist())
                .andExpect(jsonPath("$.particiones[1].offsetConfirmado").doesNotExist())
                .andExpect(jsonPath("$.particiones[1].offsetFinal").value(800));
    }

    @Test
    @DisplayName("200 · el listado de grupos con su estado")
    void listado200() throws Exception {
        when(servicio.listar()).thenReturn(List.of(new GrupoResumen(GRUPO, "Stable", false)));

        mockMvc.perform(comoAdmin(get(RUTA)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].id").value(GRUPO))
                .andExpect(jsonPath("$[0].estado").value("Stable"));
    }

    @Test
    @DisplayName("404 · un grupo que el cluster no conoce")
    void grupoInexistenteEs404() throws Exception {
        when(servicio.describir("inventado"))
                .thenThrow(new RecursoNoEncontradoException("grupo", "inventado"));

        mockMvc.perform(comoAdmin(get(RUTA + "/inventado")))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("RECURSO_NO_ENCONTRADO"))
                .andExpect(jsonPath("$.recurso").value("grupo"));
    }

    // ── Baja ─────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("204 · eliminar un grupo sin miembros")
    void eliminar204() throws Exception {
        when(servicio.eliminar(GRUPO)).thenReturn(
                OperacionResultado.de("grupo", GRUPO, "eliminado", "eliminado"));

        mockMvc.perform(comoAdmin(delete(RUTA + "/" + GRUPO)))
                .andExpect(status().isNoContent());
    }

    @Test
    @DisplayName("409 · eliminar un grupo con consumidores conectados")
    void eliminarActivoEs409() throws Exception {
        when(servicio.eliminar(GRUPO)).thenThrow(
                new GrupoActivoException(GRUPO, "Stable", List.of("audit-1"), "eliminar"));

        mockMvc.perform(comoAdmin(delete(RUTA + "/" + GRUPO)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("GRUPO_ACTIVO"))
                .andExpect(jsonPath("$.estado").value("Stable"))
                .andExpect(jsonPath("$.miembros[0]").value("audit-1"));
    }

    // ── Reposicionamiento ────────────────────────────────────────────────────

    @Test
    @DisplayName("200 · reposicionar a earliest")
    void resetEarliest200() throws Exception {
        when(servicio.reposicionar(eq(GRUPO), any())).thenReturn(
                OperacionResultado.de("grupo", GRUPO, "reposicionado",
                        "se movieron 3 partición(es) con la estrategia earliest"));

        mockMvc.perform(comoAdmin(post(RUTA + "/" + GRUPO + "/offsets/reset")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"estrategia\":\"earliest\"}")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.operacion").value("reposicionado"));
    }

    @Test
    @DisplayName("200 · reposicionar con offsets explícitos por partición")
    void resetExplicito200() throws Exception {
        when(servicio.reposicionar(eq(GRUPO), any())).thenReturn(
                OperacionResultado.de("grupo", GRUPO, "reposicionado", "se movieron 1"));

        mockMvc.perform(comoAdmin(post(RUTA + "/" + GRUPO + "/offsets/reset")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"estrategia":"explicito",
                                 "offsets":[{"topico":"reservations.events","particion":0,
                                             "offset":120}]}""")))
                .andExpect(status().isOk());
    }

    @Test
    @DisplayName("409 · reposicionar un grupo en marcha, con el motivo explicado")
    void resetConGrupoActivoEs409() throws Exception {
        when(servicio.reposicionar(eq(GRUPO), any())).thenThrow(new GrupoActivoException(
                GRUPO, "Stable", List.of("audit-1", "audit-2"), "mover los offsets de"));

        mockMvc.perform(comoAdmin(post(RUTA + "/" + GRUPO + "/offsets/reset")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"estrategia\":\"latest\"}")))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("GRUPO_ACTIVO"))
                // El mensaje tiene que decir qué hacer, o el 409 no se distingue de un fallo.
                .andExpect(jsonPath("$.detalle").value(
                        org.hamcrest.Matchers.containsString("detener sus consumidores")));
    }

    @Test
    @DisplayName("400 · una estrategia que no es ninguna de las tres")
    void estrategiaInvalidaEs400() throws Exception {
        mockMvc.perform(comoAdmin(post(RUTA + "/" + GRUPO + "/offsets/reset")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"estrategia\":\"alprincipio\"}")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("VALIDACION"))
                .andExpect(jsonPath("$.campos.estrategia").value(
                        org.hamcrest.Matchers.containsString("earliest")));

        verify(servicio, never()).reposicionar(anyString(), any());
    }

    @Test
    @DisplayName("400 · un offset negativo en la lista explícita")
    void offsetNegativoEs400() throws Exception {
        mockMvc.perform(comoAdmin(post(RUTA + "/" + GRUPO + "/offsets/reset")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"estrategia":"explicito",
                                 "offsets":[{"topico":"reservations.events","particion":0,
                                             "offset":-5}]}""")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("VALIDACION"));

        verify(servicio, never()).reposicionar(anyString(), any());
    }

    @Test
    @DisplayName("400 · la incoherencia entre la estrategia y los offsets llega como 400")
    void incoherenciaEs400() throws Exception {
        when(servicio.reposicionar(eq(GRUPO), any())).thenThrow(new IllegalArgumentException(
                "la estrategia explicito necesita la lista de offsets"));

        mockMvc.perform(comoAdmin(post(RUTA + "/" + GRUPO + "/offsets/reset")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"estrategia\":\"explicito\"}")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("PETICION_INVALIDA"));
    }
}
