package ru.x5;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.GZIPOutputStream;

/**
 * Flink-джоба для отправки тестового XML в Kafka.
 *
 * Использование:
 * 1. Вставь свой XML в переменную TEST_XML
 * 2. Укажи адрес Kafka и название топика
 * 3. Засабмить через Flink UI
 *
 * Джоба отправит XML (закодированный как gzip+base64) в указанный топик и завершится.
 */
public class TestProducerJob {

    // ============ НАСТРОЙКИ ============

    // Адрес Kafka
    private static final String BOOTSTRAP_SERVERS = "kafka-kafka-bootstrap.kafka.svc.cluster.local:9092";

    // Тестовый топик
    private static final String TOPIC = "xml-documents";

    // Сколько раз отправить (2 = отправит дубль для проверки дедупликации)
    private static final int SEND_COUNT = 2;

    // XML читается из файла src/main/resources/test.xml (попадает в JAR)
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

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Кодируем XML: gzip → base64
        String encoded = compressAndEncode(TEST_XML);

        // Создаём список сообщений для отправки
        String[] messages = new String[SEND_COUNT];
        for (int i = 0; i < SEND_COUNT; i++) {
            messages[i] = encoded;
        }

        // Kafka sink
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        // Отправляем
        env.fromElements(messages)
                .sinkTo(sink);

        env.execute("Test Producer - send " + SEND_COUNT + " messages to " + TOPIC);
    }

    private static String compressAndEncode(String xml) throws Exception {
        // 1. gzip
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
            gzip.write(xml.getBytes("UTF-8"));
        }
        // 2. base64
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }
}
