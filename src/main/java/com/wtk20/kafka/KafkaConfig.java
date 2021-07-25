package com.wtk20.kafka;

/**
 * @author nhannv
 */
public class KafkaConfig {

    private String brokers;
    private String clientId;
    private String appName;

    public String getBrokers() {
        return brokers;
    }

    public KafkaConfig setBrokers(String brokers) {
        this.brokers = brokers;
        return this;
    }

    public String getClientId() {
        return clientId;
    }

    public KafkaConfig setClientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public String getAppName() {
        return appName;
    }

    public KafkaConfig setAppName(String appName) {
        this.appName = appName;
        return this;
    }

}
