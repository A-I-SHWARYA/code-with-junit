package com.nest.bluehydrogen.kafkaFlinkMongo.flink;

import com.nest.bluehydrogen.kafkaFlinkMongo.clear.ClearTopicMsg;
import com.nest.bluehydrogen.kafkaFlinkMongo.sink.MongoDbSink;
import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.bson.Document;
import org.springframework.stereotype.Component;

import java.util.Properties;
@Component
@Getter
public class FlinkConsumer {
    @Value("${kafka.bootstrap.servers}")
    private String kafkaBootstrapServers;

    @Value("${kafka.group.id}")
    private String kafkaGroupId;

    @Value("${kafka.zookeeper.connect}")
    private String kafkaZookeeperConnect;

    @Value("${kafka.topic}")
    private String kafkaTopic;

    @Value("${mongodb.uri}")
    private String mongodbUri;

    @Value("${mongodb.database}")
    private String mongodbDatabase;

    @Value("${mongodb.collection}")
    private String mongodbCollection;
    @Value("${flink.job.name}")
    private String flinkJobName;
    public void start(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers", kafkaBootstrapServers);
        consumerProperties.setProperty("group.id", kafkaGroupId);
        consumerProperties.setProperty("zookeeper.connect", kafkaZookeeperConnect);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(kafkaTopic, new SimpleStringSchema(), consumerProperties);
        consumer.setStartFromEarliest();
        DataStream<String> dataStream = env.addSource(consumer);
        DataStream<Document> transformedDataStream = dataStream.map(value -> Document.parse(value));
        transformedDataStream.addSink(new MongoDbSink(mongodbUri, mongodbDatabase, mongodbCollection));
        ClearTopicMsg clearTopicMsg=new ClearTopicMsg();
        try {
            env.execute(flinkJobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
