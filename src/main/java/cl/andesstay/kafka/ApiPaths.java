package cl.andesstay.kafka;

/**
 * Prefijos de las rutas HTTP, en un solo sitio.
 *
 * Se usan constantes y no @Value porque @RequestMapping exige una constante en tiempo de
 * compilacion. La concatenacion de dos `static final String` literales lo es; el valor de un
 * @Value se resuelve en ejecucion y no compilaria.
 *
 * Alternativa descartada: server.servlet.context-path o spring.mvc.servlet.path. Las dos remapean
 * el DispatcherServlet completo, y Actuator se sirve a traves de el, asi que /actuator/health
 * pasaria a /api/v1/actuator/health y rompria el healthcheck del contenedor. Con una constante,
 * Actuator queda fuera del prefijo por construccion.
 */
public final class ApiPaths {

    public static final String V1 = "/api/v1";
    public static final String KAFKA = V1 + "/kafka";

    public static final String TOPICS = KAFKA + "/topics";
    public static final String CONSUMER_GROUPS = KAFKA + "/consumer-groups";
    public static final String PRUEBAS = KAFKA + "/pruebas";

    private ApiPaths() {
    }
}
