package cl.andesstay.kafka.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.util.Map;

/**
 * Petición de alta de un tópico.
 *
 * Las particiones y el factor de replicación son objetos y no primitivos a propósito: así se
 * distingue «no lo envié» de «lo envié en cero», y los omitidos toman el valor por omisión del
 * cluster de AndesStay en lugar de un cero que el cluster rechazaría. Es la misma razón por la que
 * los booleanos de ms-andesstay-mq son Boolean.
 */
public record CrearTopicoRequest(

        @NotBlank(message = "el nombre del tópico es obligatorio")
        @Size(max = Restricciones.LARGO_MAXIMO, message = Restricciones.MENSAJE_LARGO)
        @Pattern(regexp = Restricciones.CARACTERES_DE_NOMBRE,
                message = Restricciones.MENSAJE_CARACTERES_DE_NOMBRE)
        String nombre,

        /**
         * Tres si se omite, que es lo que usan los tópicos del caso.
         *
         * Conviene elegirlo con cuidado: es el único parámetro de un tópico que se puede subir
         * después pero no bajar nunca.
         */
        @Min(value = 1, message = "tiene que ser al menos 1")
        @Max(value = Restricciones.MAXIMO_PARTICIONES,
                message = "esta API no crea más de " + Restricciones.MAXIMO_PARTICIONES
                        + " particiones de una vez")
        Integer particiones,

        /**
         * Tres si se omite, que es el número de brokers del cluster.
         *
         * Pedir más réplicas que brokers hay hace fallar la creación entera con
         * InvalidReplicationFactor, y con min.insync.replicas en 2 un factor de 1 deja un tópico
         * que no acepta publicaciones con acks=all.
         */
        @Min(value = 1, message = "tiene que ser al menos 1")
        @Max(value = Restricciones.MAXIMO_REPLICACION,
                message = "no puede pasar de " + Restricciones.MAXIMO_REPLICACION)
        Integer factorDeReplicacion,

        /**
         * Configuraciones del tópico: retention.ms, cleanup.policy, max.message.bytes,
         * min.insync.replicas, segment.bytes...
         *
         * Se pasan tal cual al cluster, que es quien valida los nombres y los rangos; replicar
         * aquí su tabla de configuraciones sería una copia condenada a quedarse atrás.
         */
        Map<String, String> configuraciones
) {

    /** Las particiones de los tópicos del caso: reservations.events y audit.timeline tienen 3. */
    public static final int PARTICIONES_POR_OMISION = 3;

    /** Los tres brokers del compose, que es el máximo que el cluster puede garantizar. */
    public static final short REPLICACION_POR_OMISION = 3;

    public int particionesOPorOmision() {
        return particiones == null ? PARTICIONES_POR_OMISION : particiones;
    }

    public short factorOPorOmision() {
        return factorDeReplicacion == null
                ? REPLICACION_POR_OMISION
                : factorDeReplicacion.shortValue();
    }

    public Map<String, String> configuracionesOVacias() {
        return configuraciones == null ? Map.of() : configuraciones;
    }
}
