package com.nest.bluehydrogen.kafkaFlinkMongo;

import com.nest.bluehydrogen.kafkaFlinkMongo.flink.FlinkConsumer;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;
import org.springframework.context.ApplicationContext;
@RunWith(MockitoJUnitRunner.class)
@SpringBootTest
class KafkaFlinkMongoApplicationTests {
	@InjectMocks
	private KafkaFlinkMongoApplication kafkaFlinkMongoApplication;
	@Mock
	private FlinkConsumer flinkConsumer;
	@Autowired
	private FlinkConsumer flink;
	@Mock
	ApplicationContext context;

	@Mock
	KafkaFlinkMongoApplication kafkaFlinkMongoApplication1;

	@InjectMocks
	private KafkaFlinkMongoApplication mainClass = new KafkaFlinkMongoApplication();

	@Test
	public void testMainMethod() {
		when(context.getBean(KafkaFlinkMongoApplication.class)).thenReturn(kafkaFlinkMongoApplication1);
		String[] args = new String[0];
		mainClass.main(args);
		verify(kafkaFlinkMongoApplication1).run();
	}
	@Test
	void contextLoads(){
		kafkaFlinkMongoApplication.run();
		verify(flinkConsumer, times(1)).start();
		flinkConsumer.start();
	}

	@Test
	void testApplicationProperties(){
		assertEquals("kafka-demo", flink.getKafkaGroupId());
		assertEquals("localhost:9092", flink.getKafkaBootstrapServers());
		assertEquals("test", flink.getKafkaTopic());
		assertEquals("mongodb://localhost:27017", flink.getMongodbUri());
		assertEquals("patient-db", flink.getMongodbDatabase());
		assertEquals("patient-details", flink.getMongodbCollection());
		assertEquals("Stream Processor", flink.getFlinkJobName());
	}
	@Test
	public void testStart() {

	}

}

