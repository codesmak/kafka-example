package com.codesmak.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

@Component
public class KafkaConsumer {

    private CountDownLatch latch = new CountDownLatch(1);

    public CountDownLatch getLatch() {
        return latch;
    }

    @KafkaListener(topics = "${kafka.topic.boot}")
    public void receive(ConsumerRecord<?, ?> consumerRecord) {
        System.out.println("received payload='{}'" + consumerRecord.toString());
        latch.countDown();
    }


}
