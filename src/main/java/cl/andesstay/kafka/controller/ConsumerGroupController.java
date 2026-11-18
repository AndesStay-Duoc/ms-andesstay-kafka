package cl.andesstay.kafka.controller;

import cl.andesstay.kafka.ApiPaths;
import cl.andesstay.kafka.dto.GrupoDetalle;
import cl.andesstay.kafka.dto.GrupoResumen;
import cl.andesstay.kafka.dto.OperacionResultado;
import cl.andesstay.kafka.dto.ResetOffsetsRequest;
import cl.andesstay.kafka.service.ConsumerGroupService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Grupos de consumidores: estado, lag, distribución del consumo y reposicionamiento. */
@RestController
@RequestMapping(ApiPaths.CONSUMER_GROUPS)
@PreAuthorize("hasRole('Admin')")
@RequiredArgsConstructor
public class ConsumerGroupController {

    private final ConsumerGroupService servicio;

    /** 200 · Lista los grupos que el cluster conoce, con su estado. */
    @GetMapping
    public List<GrupoResumen> listar() {
        return servicio.listar();
    }

    /**
     * 200 · El detalle del grupo: miembros, offsets, LAG por partición y total, y distribución del
     * consumo.
     *
     * Es la consulta que resume de qué va este servicio. Reúne en una llamada lo que son tres
     * operaciones distintas del AdminClient y la resta que Kafka no hace por nadie.
     */
    @GetMapping("/{id}")
    public GrupoDetalle describir(@PathVariable String id) {
        return servicio.describir(id);
    }

    /**
     * 204 · Elimina el grupo y sus offsets confirmados.
     *
     * Responde 409 si el grupo tiene miembros conectados: Kafka no permite borrar un grupo en
     * marcha. Los mensajes de los tópicos no se tocan.
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> eliminar(@PathVariable String id) {
        servicio.eliminar(id);
        return ResponseEntity.noContent().build();
    }

    /**
     * 200 · Reposiciona los offsets del grupo: earliest, latest o uno explícito por partición.
     *
     * Responde 409 si el grupo tiene miembros conectados. No es una restricción de esta API:
     * Kafka no permite mover los offsets de un grupo activo, porque moverlos bajo los pies de un
     * consumidor que está leyendo produciría eventos duplicados o saltados sin que nadie se
     * entere. Hay que detener los consumidores y esperar a que el grupo quede en Empty.
     */
    @PostMapping("/{id}/offsets/reset")
    public OperacionResultado reposicionar(
            @PathVariable String id,
            @Valid @RequestBody ResetOffsetsRequest peticion) {
        return servicio.reposicionar(id, peticion);
    }
}
