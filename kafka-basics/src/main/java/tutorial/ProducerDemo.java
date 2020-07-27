package tutorial;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        final Logger logger= LoggerFactory.getLogger(ProducerDemo.class);
        System.out.println("Producer started");
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //properties for safe producer

        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,String.valueOf(Integer.MAX_VALUE));

        //properties for high throughput producer with some delay
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"5");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));



        for(int i=0;i<10;i++) {


            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Message "+i);

            producer.send(record, new Callback() {
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e == null) {
                                logger.info("Message Received"+ "\n" + recordMetadata.topic()
                                        + "\n" + recordMetadata.offset() + "\n" + recordMetadata.partition()
                                        +"\n" + recordMetadata.timestamp());
                            } else {
                                logger.error("error producing message" + e.getMessage());
                            }
                        }
                    }


            );
            producer.flush();
        }
    }
}
