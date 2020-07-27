package com.kafka.elasticsearch;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumerIdempodent {


    public static void main(String[] args) throws IOException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumerIdempodent.class);
        RestHighLevelClient client = createClient();
        KafkaConsumer<String,String>consumer=createConsumer();

        //Idempodent consumer - atleast once strategy which is enabled by default - use ID to consume into kafka to avaoid duplicates



        while(true){
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(1000));
            logger.info("**** Records received   :"+String.valueOf(records.count()));
            //to commit the data in bulk to elastic search to increase the throughput
            BulkRequest bulkRequest=new BulkRequest();
            records.forEach(e->{
                IndexRequest indexRequest=new IndexRequest("twitter1","tweets",getId(e.value())).source(e.value(), XContentType.JSON);
                bulkRequest.add(indexRequest);
            });

            if(records.count()>0) {

                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("Committing the offsets...");
                consumer.commitSync();
                logger.info("offsets committed...");

            }

        }


//        client.close();
        //close client
    }

    public static String getId(String jsonValue){
        JsonParser jsonParser=new JsonParser();
        return jsonParser.parse(jsonValue).getAsJsonObject().get("id_str").getAsString();

    }

    public static RestHighLevelClient createClient(){

        CredentialsProvider cp = new BasicCredentialsProvider();
        String userName="4fcpxlpbwh";
        String password="5277mo03wp";
        String hostName="kafka-elastic-search-9726055991.ap-southeast-2.bonsaisearch.net";
        cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName,password));



        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost(hostName, 443, "https"))
                        .setHttpClientConfigCallback(
                                httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)));

        return restHighLevelClient;



    }

    public static KafkaConsumer<String,String> createConsumer(){


        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "twitter_consumer");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // to make the processing Idempotent commit the offsets only after processing
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"10");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("twitter_tweats"));

        return consumer;


    }
}
