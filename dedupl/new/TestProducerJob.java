package ru.x5;

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
 * Использует обычный KafkaProducer (без Flink).
 *
 * Использование:
 * 1. Положи XML в src/main/resources/test.xml
 * 2. Запусти через Flink UI как обычную джобу
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

    // XML читается из src/main/resources/test.xml
    private static final String TEST_XML;
    static {
        try (InputStream is = TestProducerJob.class.getResourceAsStream("/test.xml")) {
            if (is == null) throw new RuntimeException("test.xml not found in resources!");
            TEST_XML = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("Cannot read test.xml", e);
        }
    }

    // ===================================

    public static void main(String[] args) throws Exception {

        // Кодируем XML: gzip → base64
        String encoded = compressAndEncode(TEST_XML);

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

        System.out.println("Done! Sent " + SEND_COUNT + " messages to topic: " + TOPIC);
    }

    private static String compressAndEncode(String xml) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
            gzip.write(xml.getBytes(StandardCharsets.UTF_8));
        }
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }
}
