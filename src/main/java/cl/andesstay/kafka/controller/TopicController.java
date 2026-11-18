package cl.andesstay.kafka.controller;

import cl.andesstay.kafka.ApiPaths;
import cl.andesstay.kafka.dto.AumentarParticionesRequest;
import cl.andesstay.kafka.dto.CrearTopicoRequest;
import cl.andesstay.kafka.dto.ModificarConfiguracionRequest;
import cl.andesstay.kafka.dto.OperacionResultado;
import cl.andesstay.kafka.dto.TopicoDetalle;
import cl.andesstay.kafka.dto.TopicoResumen;
import cl.andesstay.kafka.service.TopicService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Tópicos: creación, listado, detalle, configuración, particiones y borrado.
 *
 * El prefijo sale de la constante ApiPaths.TOPICS, que es el único @RequestMapping del recurso:
 * los demás mappings son relativos a él, así que una futura v2 se declara en un solo sitio.
 *
 * El rol Admin se exige a nivel de clase. Al estar junto al recurso y no en la cadena de filtros,
 * un método nuevo no puede nacer sin protección por haberse olvidado de añadir su pathMatcher.
 */
@RestController
@RequestMapping(ApiPaths.TOPICS)
@PreAuthorize("hasRole('Admin')")
@RequiredArgsConstructor
public class TopicController {

    private final TopicService servicio;

    /**
     * 201 · Crea el tópico.
     *
     * No es idempotente, al contrario que el alta de una cola en ms-andesstay-mq: repetir la
     * petición responde 409 aunque la definición coincida, porque es lo que hace createTopics.
     */
    @PostMapping
    public ResponseEntity<OperacionResultado> crear(@Valid @RequestBody CrearTopicoRequest peticion) {
        return ResponseEntity.status(HttpStatus.CREATED).body(servicio.crear(peticion));
    }

    /**
     * 200 · Lista los tópicos con sus particiones, réplicas y réplicas al día.
     *
     * @param incluirInternos true para ver también __consumer_offsets y los tópicos de trabajo de
     *                        Kafka Streams. Por omisión se esconden: son ruido en un listado de
     *                        administración y ninguno se administra a mano
     */
    @GetMapping
    public List<TopicoResumen> listar(
            @RequestParam(defaultValue = "false") boolean incluirInternos) {
        return servicio.listar(incluirInternos);
    }

    /**
     * 200 · El detalle del tópico: líder, réplicas e ISR por partición, y su configuración
     * efectiva.
     */
    @GetMapping("/{nombre}")
    public TopicoDetalle describir(@PathVariable String nombre) {
        return servicio.describir(nombre);
    }

    /**
     * 200 · Modifica configuraciones del tópico.
     *
     * Es un PUT sobre un subrecurso y no un PATCH aunque el cambio sea incremental: lo que se
     * reemplaza es el valor de las claves enviadas, que es exactamente lo que el cuerpo describe.
     * Las claves que no llegan se quedan como estaban.
     *
     * Se permite también sobre los tópicos protegidos: un cambio de configuración es reversible y
     * un borrado no.
     */
    @PutMapping("/{nombre}/config")
    public OperacionResultado configurar(
            @PathVariable String nombre,
            @Valid @RequestBody ModificarConfiguracionRequest peticion) {
        return servicio.modificarConfiguracion(nombre, peticion);
    }

    /**
     * 200 · Sube el número de particiones del tópico hasta el total indicado.
     *
     * Es un POST sobre un subrecurso y no un PUT sobre el tópico porque no reemplaza nada: agrega
     * particiones a las que ya hay. Pedir un total menor o igual al actual responde 400 con la
     * explicación de por qué Kafka no deja bajarlas.
     */
    @PostMapping("/{nombre}/particiones")
    public OperacionResultado aumentarParticiones(
            @PathVariable String nombre,
            @Valid @RequestBody AumentarParticionesRequest peticion) {
        return servicio.aumentarParticiones(nombre, peticion);
    }

    /**
     * 204 · Elimina el tópico y el log de todas sus particiones.
     *
     * Los nombres de la topología de AndesStay responden 409: eliminar reservations.events corta
     * el flujo de eventos hacia ms-andesstay-audit, y en Kafka es irreversible.
     */
    @DeleteMapping("/{nombre}")
    public ResponseEntity<Void> eliminar(@PathVariable String nombre) {
        servicio.eliminar(nombre);
        return ResponseEntity.noContent().build();
    }
}
