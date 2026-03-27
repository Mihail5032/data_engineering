package ru.x5;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

/**
 * Тестовый продюсер для отправки XML в Kafka.
 * Завёрнут в Flink-джобу чтобы можно было запустить через Flink UI.
 *
 * Использование:
 * 1. Положи XML в src/main/resources/test.xml
 * 2. Собери jar: mvn clean package -DskipTests
 * 3. Запусти через Flink UI
 *    Entry class: ru.x5.TestProducerJob
 */
public class TestProducerJob {

    // ============ НАСТРОЙКИ ============

    // Адрес Kafka
    private static final String BOOTSTRAP_SERVERS = "kafka-kafka-bootstrap.kafka.svc.cluster.local:9092";

    // Тестовый топик
    private static final String TOPIC = "xml-documents";

    // Сколько раз отправить (2 = дубль для проверки дедупликации)
    private static final int SEND_COUNT = 2;

    // ===================================

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements("start")
                .map(new KafkaSenderFunction())
                .setParallelism(1)
                .print();

        env.execute("Test Producer Job");
    }

    // ===================================

    public static class KafkaSenderFunction extends RichMapFunction<String, String> {

        @Override
        public String map(String value) throws Exception {
            // Читаем XML из ресурсов
            String xml;
            try (InputStream is = KafkaSenderFunction.class.getResourceAsStream("/test.xml")) {
                if (is == null) throw new RuntimeException("test.xml not found in resources!");
                xml = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            }

            // Кодируем XML: gzip → base64
            String encoded = compressAndEncode(xml);

            // Настройки Kafka продюсера
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            // Отправляем SEND_COUNT раз
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                for (int i = 0; i < SEND_COUNT; i++) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, encoded);
                    producer.send(record).get();
                    System.out.println("Sent message " + (i + 1) + " of " + SEND_COUNT);
                }
            }

            return "Done! Sent " + SEND_COUNT + " messages to topic: " + TOPIC;
        }
    }

    private static String compressAndEncode(String xml) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
            gzip.write(xml.getBytes(StandardCharsets.UTF_8));
        }
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }
}
