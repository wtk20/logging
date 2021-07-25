package com.wtk20.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author nhannv
 */
public class KafkaProducer {

    private static KafkaConfig kafkaConfig;

    public static void init(KafkaConfig config) {
        if (kafkaConfig == null) {
            throw new IllegalArgumentException("Kafka config is NULL.");
        }

        String brokers = config.getBrokers();
        if (brokers == null || brokers.trim().isEmpty()) {
            throw new IllegalArgumentException("Brokers config was not found.");
        }

        String clientId = config.getClientId();
        if (clientId == null || clientId.trim().isEmpty()) {
            throw new IllegalArgumentException("ClientId config was not found.");
        }

        String appName = config.getAppName();
        if (appName == null || appName.trim().isEmpty()) {
            throw new IllegalArgumentException("AppName config was not found.");
        }

        kafkaConfig = config;
    }

    private Producer<String, String> createProducer() {
        if (kafkaConfig == null) {
            throw new IllegalArgumentException("Kafka config is NULL.");
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBrokers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaConfig.getClientId());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    }

    public boolean produce(String topicName, String requestId, String msg) {
        try (Producer<String, String> producer = this.createProducer()) {
            producer.send(new ProducerRecord<>(topicName, requestId, msg));
            return true;
        }
    }
}
