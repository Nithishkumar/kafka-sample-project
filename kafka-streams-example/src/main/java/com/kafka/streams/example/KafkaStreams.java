package com.kafka.streams.example;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KafkaStreams {

    public static void main(String[] args) {

        //properties
        Properties properties=new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"kafka-streams");


        //create topology
        StreamsBuilder streamsBuilder=new StreamsBuilder();

        //input-topic
        KStream<String,String> inputTopic=streamsBuilder.stream("twitter_tweats");
        KStream<String,String> filteredStream=inputTopic.filter((k,v)->
            checkForFollowers(v)>10000);

        //output topic
        filteredStream.to("important_tweets");


        //build topology
        org.apache.kafka.streams.KafkaStreams kafkaStreams=new org.apache.kafka.streams.KafkaStreams(streamsBuilder.build(),properties);
        kafkaStreams.start();




        //start

    }

    private static int checkForFollowers(String v) {

        JsonParser jsonParser=new JsonParser();
        return jsonParser.parse(v).getAsJsonObject().get("user").getAsJsonObject().get("followers_count").getAsInt();
    }
}
