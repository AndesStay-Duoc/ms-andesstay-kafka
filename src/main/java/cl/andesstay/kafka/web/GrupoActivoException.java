package cl.andesstay.kafka.web;

import java.util.List;

/**
 * 409 · Se intentó mover o eliminar los offsets de un grupo que tiene miembros conectados.
 *
 * Kafka no permite ninguna de las dos cosas sobre un grupo activo, y el motivo no es un capricho:
 * los offsets confirmados son estado compartido del grupo, y moverlos bajo los pies de un
 * consumidor que está leyendo produciría eventos duplicados o saltados sin que nadie se entere.
 * El cluster responde a `alterConsumerGroupOffsets` con UnknownMemberId y a `deleteConsumerGroups`
 * con GroupNotEmpty, dos errores que por sí solos no explican nada a quien llama.
 *
 * Para reposicionar un grupo hay que detener antes sus consumidores y esperar a que el estado pase
 * a Empty, que es lo que dice el mensaje.
 */
public class GrupoActivoException extends RuntimeException {

    private final String grupo;
    private final String estado;
    private final List<String> miembros;

    public GrupoActivoException(String grupo, String estado, List<String> miembros, String operacion) {
        super("no se puede " + operacion + " el grupo '" + grupo + "': está en estado " + estado
                + " con " + miembros.size() + " miembro(s) conectado(s). "
                + "Kafka no permite mover los offsets de un grupo activo; "
                + "hay que detener sus consumidores y esperar a que quede en Empty.");
        this.grupo = grupo;
        this.estado = estado;
        this.miembros = List.copyOf(miembros);
    }

    public String getGrupo() {
        return grupo;
    }

    public String getEstado() {
        return estado;
    }

    public List<String> getMiembros() {
        return miembros;
    }
}
