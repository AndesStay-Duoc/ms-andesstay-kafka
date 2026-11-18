package cl.andesstay.kafka.config;

import cl.andesstay.kafka.security.AzureRolesConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Validación del JWT de Entra ID y exigencia del rol Admin.
 *
 * Este servicio sí lleva starter de security, al igual que ms-andesstay-mq: la superficie HTTP son
 * operaciones que eliminan tópicos y mueven offsets de grupos en marcha. Un DELETE sobre
 * reservations.events se lleva el log entero de sus tres particiones, y un reset de offsets sobre
 * el grupo de ms-andesstay-audit le hace reprocesar o saltarse eventos de reserva.
 *
 * El rol se exige con @PreAuthorize en los controladores y no con un pathMatcher por ruta: las dos
 * familias de recursos piden exactamente lo mismo, y tenerlo junto al método evita que una ruta
 * nueva nazca sin protección por haberse olvidado de la cadena.
 */
@Configuration
@EnableWebSecurity
@EnableMethodSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
            // Sin estado y sin formularios: es una API con Bearer token, no hay sesión que
            // proteger de un CSRF.
            .csrf(csrf -> csrf.disable())
            .sessionManagement(sm -> sm.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .authorizeHttpRequests(auth -> auth
                // Solo health. El healthcheck del contenedor lo consulta sin token, y es lo único
                // que queda abierto: /actuator/metrics describiría el cluster a cualquiera.
                .requestMatchers("/actuator/health").permitAll()
                .anyRequest().authenticated()
            )
            .oauth2ResourceServer(oauth2 -> oauth2
                .jwt(jwt -> jwt.jwtAuthenticationConverter(jwtAuthenticationConverter()))
            );
        return http.build();
    }

    /**
     * Traduce el claim "roles" del token a authorities con prefijo ROLE_, que es lo que
     * hasRole('Admin') espera encontrar.
     */
    @Bean
    public JwtAuthenticationConverter jwtAuthenticationConverter() {
        JwtAuthenticationConverter convertidor = new JwtAuthenticationConverter();
        convertidor.setJwtGrantedAuthoritiesConverter(new AzureRolesConverter());
        return convertidor;
    }
}
