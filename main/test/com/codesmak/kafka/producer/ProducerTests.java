package com.codesmak.kafka.producer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;


@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class ProducerTests {
    
    private KafkaTemplate<Integer,String> template;
    private KafkaMessageListenerContainer<ConsumerFactory<Integer,String>,ContainerProperties> container;
    private BlockingQueue<ConsumerRecord<Integer, String>> records;
    private static final String producerQueue = "producer.q";

    @ClassRule
    public static KafkaEmbedded kafka = new KafkaEmbedded(1,true,producerQueue);

    @Before
    public void setup() throws Exception {

        Map<String,Object> consumerProps = KafkaTestUtils.consumerProps("123","true",kafka);

        records = new LinkedBlockingQueue<>();

        container = new KafkaMessageListenerContainer(getConsumerFactory(consumerProps),new ContainerProperties(producerQueue));
        container.setupMessageListener(new MessageListener<Integer, String>() {
            @Override
            public void onMessage(ConsumerRecord<Integer, String> record) {
                System.out.println("test-listener received message='{}'"+record.toString());
                records.add(record);
            }
        });
        container.start();

        ContainerTestUtils.waitForAssignment(container, kafka.getPartitionsPerTopic());

    }

    @After
    public void shutdown() {
        container.stop();
    }

    @Test
    public void testProducer() throws InterruptedException {

        String kafkaRecordData = "Invoice record FSSAR";

        Map<String, Object> senderProperties = KafkaTestUtils.senderProps(kafka.getBrokersAsString());

        template = new KafkaTemplate<Integer,String>(getProducerFactory(senderProperties));
        template.send(producerQueue,23,kafkaRecordData);

        ConsumerRecord<Integer,String> kafkaRecord = records.poll(10, TimeUnit.SECONDS);

        assertEquals(kafkaRecord.key().intValue(),23);
        assertEquals(kafkaRecord.value(),kafkaRecordData);
    }


    private DefaultKafkaConsumerFactory<Integer, String> getConsumerFactory(Map<String, Object> consumerProps) {
        return new DefaultKafkaConsumerFactory<Integer, String>(consumerProps);
    }

    private DefaultKafkaProducerFactory<Integer, String> getProducerFactory(Map<String, Object> senderProperties) {
        return new DefaultKafkaProducerFactory<Integer, String>(senderProperties);
    }
}
