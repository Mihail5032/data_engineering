package ru.x5.factory;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.log4j.Logger;
import ru.x5.config.PropertiesHolder;
import ru.x5.deserializer.CustomDeserializer;

import java.net.URL;

public class KafkaSourceFactory {
    private static final Logger log = Logger.getLogger(KafkaSourceFactory.class);

    public static KafkaSource<String> buildKafkaSource() {
        PropertiesHolder pros = PropertiesHolder.getInstance();

        KafkaSourceBuilder<String> builder = KafkaSource.<String>builder()
                .setBootstrapServers(pros.getBoostrapServers())
                .setTopics(pros.getTopicName())
                .setGroupId("srv.data_stream_group_id_5")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new CustomDeserializer(String.class));

        // Kerberos только если keytab указан (для прода)
        if (!"none".equals(pros.getKeytabLocation())) {
            String keytabLocation = pros.getKeytabLocation();
            if (keytabLocation.startsWith("classpath")) {
                String keytabName = keytabLocation.split(":")[1];
                URL resource = KafkaSourceFactory.class.getClassLoader().getResource(keytabName);
                keytabLocation = resource.getPath();
            }
            String jaas = String.format(
                    "com.sun.security.auth.module.Krb5LoginModule required" +
                            " useKeyTab=true" +
                            " storeKey=true" +
                            " debug=true" +
                            " keyTab=%s" +
                            " serviceName=%s" +
                            " principal=%s;",
                    "\"" + keytabLocation + "\"",
                    "kafka",
                    "\"" + pros.getKeytabPrincipal() + "\""
            );
            log.info("JAAS FILE: " + jaas);
            builder.setProperty("bootstrap.servers", "kafka.nodes=" + pros.getBoostrapServers())
                   .setProperty("security.protocol", "SASL_PLAINTEXT")
                   .setProperty("sasl.mechanism", "GSSAPI")
                   .setProperty("sasl.jaas.config", jaas)
                   .setProperty("sasl.kerberos.service.name", "kafka");
        } else {
            log.info("Kerberos disabled, connecting to Kafka without auth");
        }

        return builder.build();
    }
}
