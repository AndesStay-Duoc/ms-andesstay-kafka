package cl.andesstay.kafka.dto;

/** Un broker del cluster, tal como lo identifica Kafka: por id numérico, host y puerto. */
public record NodoResumen(int id, String host, int puerto) {
}
