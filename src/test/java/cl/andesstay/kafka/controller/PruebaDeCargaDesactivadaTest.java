package cl.andesstay.kafka.controller;

import cl.andesstay.kafka.config.SecurityConfig;
import cl.andesstay.kafka.web.ManejadorDeErrores;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Sin el perfil demo, la prueba de carga no existe.
 *
 * Conviene tenerlo afirmado y no solo escrito en el README: la @Profile del controlador es lo
 * único que impide que un administrador pueda inyectar diez mil mensajes en un tópico de
 * producción, y es la clase de anotación que desaparece en una refactorización sin que nada
 * falle.
 *
 * La respuesta es 404 y no 403 porque no hay nada en esa ruta: el bean no se registra.
 */
@WebMvcTest(PruebaDeCargaController.class)
@Import({SecurityConfig.class, ManejadorDeErrores.class})
class PruebaDeCargaDesactivadaTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private JwtDecoder jwtDecoder;

    @Test
    @DisplayName("404 · la ruta no está registrada sin el perfil demo")
    void sinPerfilNoHayRuta() throws Exception {
        mockMvc.perform(post("/api/v1/kafka/pruebas/carga")
                        .param("count", "5")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_Admin"))))
                .andExpect(status().isNotFound());
    }
}
