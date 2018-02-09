package com.codesmak.kafka.producer;

import com.codesmak.kafka.consumer.KafkaConsumer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ConsumerTest {

    @Autowired KafkaConsumer consumer;

    @Test
    public void testConsumingTwitterData(){

        System.out.println("REC COUNT: " );

    }
}
