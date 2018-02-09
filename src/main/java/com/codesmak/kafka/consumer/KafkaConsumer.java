package com.codesmak.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

@Component
public class KafkaConsumer {

    public final CountDownLatch latch1 = new CountDownLatch(1);

    @KafkaListener(id = "foo1", topics = "annotated1")
    public void listen1(String foo) {
        this.latch1.countDown();
    }
}
