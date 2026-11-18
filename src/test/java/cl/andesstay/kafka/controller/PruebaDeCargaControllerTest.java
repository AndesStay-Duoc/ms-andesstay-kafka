package cl.andesstay.kafka.controller;

import cl.andesstay.kafka.config.KafkaProperties;
import cl.andesstay.kafka.config.SecurityConfig;
import cl.andesstay.kafka.web.ManejadorDeErrores;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * La prueba de carga, con el perfil demo activo.
 *
 * Lo que se afirma aquí no es el rendimiento —eso depende de la máquina y no se prueba sin
 * cluster— sino que la medición esté bien hecha: que se espere la confirmación de todos los
 * envíos antes de parar el cronómetro, que el lote pendiente se vacíe, y que un envío fallido se
 * cuente en lugar de inflar el throughput.
 */
@WebMvcTest(PruebaDeCargaController.class)
@Import({SecurityConfig.class, ManejadorDeErrores.class,
        PruebaDeCargaControllerTest.Propiedades.class})
@ActiveProfiles("demo")
class PruebaDeCargaControllerTest {

    private static final String RUTA = "/api/v1/kafka/pruebas/carga";
    private static final String TOPICO_POR_OMISION = "pruebas.carga";

    /**
     * Las propiedades no las aporta el slice de web, así que se declaran aquí. Es un record: se
     * construye en lugar de simularse.
     */
    @TestConfiguration
    static class Propiedades {

        @Bean
        KafkaProperties kafkaProperties() {
            return new KafkaProperties("localhost:9092", 10_000, List.of(),
                    new KafkaProperties.Pruebas(TOPICO_POR_OMISION, 10_000));
        }
    }

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private KafkaTemplate<String, String> plantilla;

    @MockBean
    private JwtDecoder jwtDecoder;

    private static MockHttpServletRequestBuilder comoAdmin(MockHttpServletRequestBuilder peticion) {
        return peticion.with(jwt().authorities(new SimpleGrantedAuthority("ROLE_Admin")));
    }

    private void enviosCorrectos() {
        when(plantilla.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    @DisplayName("200 · publica los mensajes pedidos y devuelve el throughput")
    void publicaYMide() throws Exception {
        enviosCorrectos();

        mockMvc.perform(comoAdmin(post(RUTA).param("count", "5")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.topico").value(TOPICO_POR_OMISION))
                .andExpect(jsonPath("$.mensajes").value(5))
                .andExpect(jsonPath("$.fallidos").value(0))
                .andExpect(jsonPath("$.mensajesPorSegundo").isNumber());

        verify(plantilla, times(5)).send(eq(TOPICO_POR_OMISION), anyString(), anyString());
        // send() encola; sin el vaciado, el último lote se quedaría esperando sus linger.ms y la
        // medición incluiría esa espera.
        verify(plantilla).flush();
    }

    @Test
    @DisplayName("cada mensaje lleva una clave distinta, para repartirlos entre las particiones")
    void clavesDistintas() throws Exception {
        enviosCorrectos();

        mockMvc.perform(comoAdmin(post(RUTA).param("count", "3")))
                .andExpect(status().isOk());

        // Con una sola clave caerían todos en la misma partición y el lag se concentraría en un
        // consumidor, que es lo contrario de lo que la demostración quiere mostrar.
        verify(plantilla).send(anyString(), eq("carga-0"), anyString());
        verify(plantilla).send(anyString(), eq("carga-1"), anyString());
        verify(plantilla).send(anyString(), eq("carga-2"), anyString());
    }

    @Test
    @DisplayName("el tópico se puede indicar por parámetro")
    void topicoParametrizable() throws Exception {
        enviosCorrectos();

        mockMvc.perform(comoAdmin(post(RUTA).param("count", "2").param("topico", "otro.topico")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.topico").value("otro.topico"));

        verify(plantilla, times(2)).send(eq("otro.topico"), anyString(), anyString());
    }

    @Test
    @DisplayName("un envío que el cluster no confirma se cuenta como fallido")
    void envioFallidoSeCuenta() throws Exception {
        CompletableFuture<SendResult<String, String>> roto = new CompletableFuture<>();
        roto.completeExceptionally(new IllegalStateException("sin réplicas en sincronía"));
        when(plantilla.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null))
                .thenReturn(roto);

        mockMvc.perform(comoAdmin(post(RUTA).param("count", "2")))
                .andExpect(status().isOk())
                // Sin contarlos, el throughput mediría envíos que nunca llegaron.
                .andExpect(jsonPath("$.fallidos").value(1));
    }

    @Test
    @DisplayName("400 · publicar cero mensajes no es una prueba de carga")
    void countCeroEs400() throws Exception {
        mockMvc.perform(comoAdmin(post(RUTA).param("count", "0")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("VALIDACION"));

        verify(plantilla, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("400 · un count que no es un número")
    void countNoNumericoEs400() throws Exception {
        mockMvc.perform(comoAdmin(post(RUTA).param("count", "muchos")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("PETICION_INVALIDA"));
    }

    @Test
    @DisplayName("401 · la prueba de carga también exige token")
    void sinTokenEs401() throws Exception {
        mockMvc.perform(post(RUTA).param("count", "5")).andExpect(status().isUnauthorized());

        verify(plantilla, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("403 · un token de Cliente no inyecta diez mil mensajes en ningún tópico")
    void clienteEs403() throws Exception {
        mockMvc.perform(post(RUTA).param("count", "5")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_Cliente"))))
                .andExpect(status().isForbidden());

        verify(plantilla, never()).send(anyString(), anyString(), anyString());
    }
}
