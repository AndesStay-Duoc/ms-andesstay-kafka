package cl.andesstay.kafka.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * El productor de la prueba de carga. Solo existe con el perfil demo activo.
 *
 * Es la unica parte de este servicio que PUBLICA mensajes; todo lo demas administra. Esta detras
 * de un perfil porque un administrador que puede inyectar diez mil mensajes en un topico de
 * produccion no es un administrador, y porque en un despliegue normal el bean sobraria.
 *
 * LOS TRES VALORES DE LA GUIA 3.3.2 DEL CURSO, y por que cada uno:
 *
 *   batch.size 32 KB     cuanto acumula el productor para un mismo broker antes de enviar. El
 *                        valor por omision son 16 KB; subirlo a 32 llena menos veces la red con
 *                        lotes pequenos.
 *   linger.ms 20         cuanto espera a que el lote se llene antes de enviarlo igual. Por
 *                        omision es 0, es decir, envia en cuanto puede: sin espera no hay lotes,
 *                        y sin lotes no hay rendimiento. Son 20 ms de latencia a cambio.
 *   compression snappy   comprime cada lote. Snappy comprime menos que gzip pero cuesta mucho
 *                        menos CPU, que es el compromiso que interesa cuando lo que se mide es
 *                        cuantos mensajes por segundo entran.
 *
 * acks=all ademas, que no es de la guia: el cluster tiene min.insync.replicas=2, y con acks=1 la
 * prueba mediria un rendimiento que el sistema real no tiene porque no espera a las replicas.
 */
@Configuration
@Profile("demo")
public class ProductorDeCargaConfig {

    /** El lote que el productor acumula por broker antes de enviar, en bytes. */
    private static final int TAMANO_DE_LOTE = 32 * 1024;

    /** Lo que espera a que el lote se llene, en milisegundos. */
    private static final int ESPERA_DE_LOTE_MS = 20;

    @Bean
    public ProducerFactory<String, String> productorDeCarga(KafkaProperties propiedades) {
        Map<String, Object> configuracion = new HashMap<>();
        configuracion.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, propiedades.bootstrapServers());
        configuracion.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configuracion.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configuracion.put(ProducerConfig.BATCH_SIZE_CONFIG, TAMANO_DE_LOTE);
        configuracion.put(ProducerConfig.LINGER_MS_CONFIG, ESPERA_DE_LOTE_MS);
        configuracion.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        configuracion.put(ProducerConfig.ACKS_CONFIG, "all");
        configuracion.put(ProducerConfig.CLIENT_ID_CONFIG, "ms-andesstay-kafka-carga");
        return new DefaultKafkaProducerFactory<>(configuracion);
    }

    /**
     * Reemplaza al KafkaTemplate de la autoconfiguracion, que es @ConditionalOnMissingBean: con
     * este declarado, el de Spring Boot no se crea y no hay dos productores con configuraciones
     * distintas dando vueltas.
     */
    @Bean
    public KafkaTemplate<String, String> plantillaDeCarga(ProducerFactory<String, String> fabrica) {
        return new KafkaTemplate<>(fabrica);
    }
}
