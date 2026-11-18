package cl.andesstay.kafka.service;

/**
 * Qué cuenta como tópico interno, que es lo que el listado esconde salvo que se pida lo contrario.
 *
 * Hay dos familias y se reconocen de formas distintas, de ahí que esta decisión viva en un solo
 * sitio:
 *
 *   los del cluster      __consumer_offsets, __transaction_state. El propio Kafka los marca como
 *                        internos, y también los delata el prefijo.
 *   los de Kafka Streams los almacenes de estado (-changelog) y las particiones intermedias
 *                        (-repartition) que Streams crea para sus topologías. Para el cluster son
 *                        tópicos normales: no vienen marcados, y solo el nombre los distingue.
 *
 * _schemas va en la lista porque es el tópico donde el Schema Registry guarda los esquemas: es
 * infraestructura, no un tópico de negocio, aunque para el cluster sea uno más.
 *
 * Se esconden porque en el listado de un administrador son ruido —__consumer_offsets tiene
 * cincuenta particiones él solo— y porque ninguno se administra a mano: tocarlos rompe el cluster.
 * Siguen estando ahí con ?incluirInternos=true.
 */
final class TopicosInternos {

    private static final String PREFIJO_DEL_CLUSTER = "__";
    private static final String ESQUEMAS = "_schemas";
    private static final String SUFIJO_CHANGELOG = "-changelog";
    private static final String SUFIJO_REPARTITION = "-repartition";

    private TopicosInternos() {
    }

    static boolean esInterno(String nombre) {
        if (nombre == null) {
            return false;
        }
        return nombre.startsWith(PREFIJO_DEL_CLUSTER)
                || ESQUEMAS.equals(nombre)
                || nombre.endsWith(SUFIJO_CHANGELOG)
                || nombre.endsWith(SUFIJO_REPARTITION);
    }
}
