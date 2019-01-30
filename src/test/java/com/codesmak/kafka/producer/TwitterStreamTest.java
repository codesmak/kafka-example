package com.codesmak.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Properties;

public class TwitterStreamTest {

    @Test
    public void testTwitterStreamConnection() throws InterruptedException {

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setOAuthConsumerKey("")
                .setOAuthConsumerSecret("")
                .setOAuthAccessToken("")
                .setOAuthAccessTokenSecret("")
        .setJSONStoreEnabled(true);

        TwitterStream twitterStream = new TwitterStreamFactory(configurationBuilder.build()).getInstance();

        final Producer<String,String> producer = new KafkaProducer<String, String>(getConfig());

        twitterStream.addListener(new StatusListener() {
            @Override
            public void onException(Exception ex) {

            }

            public void onStatus(Status status) {
                String json = TwitterObjectFactory.getRawJSON(status);
                System.out.println("^^^^^^^^^^^^^^^^^"+json);
                //System.out.println(status.getText()); // print tweet text to console
                ProducerRecord<String,String> kafkaRecord = new ProducerRecord<String,String>("twitter-data",json);
                producer.send(kafkaRecord);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {

            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {

            }

            @Override
            public void onStallWarning(StallWarning warning) {

            }

        });

        FilterQuery tweetFilterQuery = new FilterQuery(); // See
        tweetFilterQuery.track(new String[]{"MIZZOU"}); // OR on keywords
        tweetFilterQuery.language(new String[]{"en"}); // Note that language does not work properly on Norwegian tweets

        twitterStream.filter(tweetFilterQuery);

        Thread.sleep(60000);
    }

    private Properties getConfig() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","localhost:9092");
        props.setProperty("batch.size","1");
        props.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("producer.type","sync");
        props.setProperty("group.id","foo");
        return props;
    }

}
