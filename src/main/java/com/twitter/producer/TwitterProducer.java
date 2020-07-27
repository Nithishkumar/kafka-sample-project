package com.twitter.producer;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {


	Logger logger= LoggerFactory.getLogger(TwitterProducer.class);


	public TwitterProducer(){
	}

	public static void main(String[] args) {
		new TwitterProducer().run();
	}


	public void run(){
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		com.twitter.hbc.core.Client client=createTwitterClient(msgQueue);
		client.connect();
		KafkaProducer<String,String> producer=createProducer();

		//shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(()->{
			logger.info("Stoping application...");
			client.stop();
			producer.close();
		}));

		while (!client.isDone()) {
			String msg=null;
			try {
				 msg = msgQueue.poll(5,TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}
			if(msg!=null){
			   logger.info("***** Message from Twitter :"+msg);
		   producer.send(new ProducerRecord<String, String>("twitter_tweats", null, msg), new Callback() {
						   @Override
						   public void onCompletion(RecordMetadata recordMetadata, Exception e) {
							   if(e!=null){
								   logger.info("Error in producer",e);
							   }else{
								   logger.info("**** Produced message ****");
							   }
						   }
					   }
			   );
			   producer.flush();
			}
		} }

	private KafkaProducer<String, String> createProducer() {

		System.out.println("Creating Producer...");
		Properties properties=new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		return producer;
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue){

		String consumerKey="CUHrCiC4yRGZllDCYsbMW3DZD";
		String consumerSecret="oGkDlG282r1Yr7uFh15HZmTOUCsKo7q6TohfhflN28pFB9YfDe";
		String token="548349461-rzJo000Qh2L09wD8AAYQMgp2dVl6rjwtFpHLYbgk";
		String secret="jDbyltl2Yr4pewZDlBAZ9VSP9RjfXflZqCVm9ImPyihHv";

		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
		List<String> terms = Lists.newArrayList("Kafka");
		hosebirdEndpoint.trackTerms(terms);
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder()
				.name("Hosebird-Client-01")                              // optional: mainly for the logs
				.hosts(hosebirdHosts)
				.authentication(hosebirdAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
// Attempts to establish a connection.

		return hosebirdClient;
	}




}
