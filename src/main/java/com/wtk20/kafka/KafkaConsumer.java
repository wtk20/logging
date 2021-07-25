package com.wtk20.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * @author nhannv
 */
public class KafkaConsumer {

    static {
        try {

        } catch (Exception e) {

        }
    }

    public void consume() {

        //Kafka consumer configuration settings
        String topicName = "test";
        //int partition = 0;
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        org.apache.kafka.clients.consumer.KafkaConsumer consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);

        //Kafka Consumer subscribes list of topics here.
        consumer.subscribe(Arrays.asList(topicName));
        //consumer.assign(Arrays.asList(new TopicPartition(topicName, partition)));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close();
        }));

        //print the topic name
        System.out.println("Assigned to topic " + topicName);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) // print the offset,key and value for the consumer records.
            {
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                //System.out.println(record.value());
            }
        }
    }
}
