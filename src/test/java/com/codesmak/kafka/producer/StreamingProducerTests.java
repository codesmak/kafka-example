package com.codesmak.kafka.producer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class StreamingProducerTests {

    private static final String dataOutTopic = "STREAM_OUT";
    private static final String dataOutTopicFoLogan = "LOGAN_OUT";
    private KafkaTemplate<Integer,String> template;
    private ProducerFactory<Integer,String> producerFactory;

    @ClassRule
    public static KafkaEmbedded kafka = new KafkaEmbedded(1,true,dataOutTopic,dataOutTopicFoLogan);


    public void prepare(){

        Map<String, Object> senderProperties = KafkaTestUtils.senderProps(kafka.getBrokersAsString());

        producerFactory = new DefaultKafkaProducerFactory<Integer, String>(senderProperties);

        template = new KafkaTemplate<Integer,String>(producerFactory);
        template.send(dataOutTopic,101,"Hello this is from test01");
        System.out.println("test01");
        System.out.println("test02");
        System.out.println("test03");
        template.send(dataOutTopic,102,"Hello this is from test02");
        template.send(dataOutTopic,103,"Hello this is from test03");
        template.send(dataOutTopic,104,"Hello this is from Logan...... HI");
    }

    @Test
    public void testStreamingProducer() throws InterruptedException {

        StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer,String> source = builder.stream(dataOutTopic);

        Predicate<Integer,String> isAboutLogan = (k,v) -> v.contains("Logan");

        source.flatMapValues(value -> Arrays.asList(value)).print();

        source.flatMapValues(value -> Arrays.asList(value)).filter(isAboutLogan).to(dataOutTopicFoLogan);

        Topology topology = builder.build();

        System.out.println("topology: "+topology.describe());

        KStream<Integer,String> sourceForLogan = builder.stream(dataOutTopicFoLogan);
        sourceForLogan.flatMapValues(value -> Arrays.asList(value)).print();

        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"stream-processing");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,kafka.getBrokersAsString());
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.Integer().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName());

        KafkaStreams streams = new KafkaStreams(topology,props);

        streams.start();

        prepare();
        prepare();
        prepare();
        prepare();
        prepare();
        prepare();

        Thread.sleep(20000);
        streams.close();

    }

}
