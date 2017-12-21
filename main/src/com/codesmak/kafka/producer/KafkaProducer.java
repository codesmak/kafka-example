package com.codesmak.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {

    @Autowired
    private KafkaTemplate<String,String> template;

    public void send(String topic, String payload){
        template.send(topic,payload);
    }

}
