package br.com.courserudemy.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoCallBack {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoCallBack.class);
        String bootstrapServers = "localhost:9092";

        //create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i< 10; i++) {
            //create a producer record
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello World "+ Integer.toString(i));

            // send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // executes every time a record is succesfuly sent or an exception is thrown

                    if (exception == null) {
                        logger.info("Received new metadata. \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "OffSet: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    } else {
                        exception.printStackTrace();
                    }
                }
            });
        }
        //flush data
        producer.flush();

        //flush and close producer
        producer.close();
    }
}
