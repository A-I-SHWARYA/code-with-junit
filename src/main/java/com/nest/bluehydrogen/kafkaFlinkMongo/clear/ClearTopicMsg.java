package com.nest.bluehydrogen.kafkaFlinkMongo.clear;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ClearTopicMsg {
    @Value("${kafka.bootstrap.servers}")
    private String kafkaBootstrapServers;
    @Value("${kafka.group.id}")
    private String kafkaGroupId;
    @Value("${kafka.topic}")
    private String kafkaTopic;
    public ClearTopicMsg(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.assign(Collections.singletonList(new TopicPartition(kafkaTopic, 0)));
        consumer.seekToBeginning(Collections.singletonList(new TopicPartition(kafkaTopic, 0)));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Clearing message with key: " + record.key() + " and value: " + record.value());
            }
            if (records.count() == 0) {
                break;
            }
        }
        consumer.close();
        System.out.println("Cleared all messages in topic: " + kafkaTopic);
    }
}
