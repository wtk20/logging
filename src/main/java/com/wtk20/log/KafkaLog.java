package com.wtk20.log;

import com.wtk20.kafka.KafkaConfig;
import com.wtk20.kafka.KafkaProducer;

/**
 * @author nhannv
 */
public enum KafkaLog {

    INSTANCE;

    private final KafkaProducer producer = new KafkaProducer();

    public void register(String brokers, String clientId) {
        KafkaConfig config = new KafkaConfig();
        config.setBrokers(brokers)
                .setClientId(clientId)
                .setAppName(System.getProperty("application.name"));
        KafkaProducer.init(config);
    }

    public void writeLog(String topicName, String requestId, String msg) {
        producer.produce(topicName, requestId, msg);
    }
}
