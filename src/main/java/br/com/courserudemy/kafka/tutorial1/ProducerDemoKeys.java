package br.com.courserudemy.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        String bootstrapServers = "localhost:9092";

        //create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i< 10; i++){
            //create a producer record

            String topic = "first_topic";
            String value = "Hello World "+ Integer.toString(i);
            String key = "id_"+ Integer.toString(i);
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key "+key);// Log the key
            //id_0 is going to partition 1
            //id_1 partition 0
            //id_2 partition 2
            //id_3 partition 0
            //id_4 partition 2
            //id_5 partition 2
            //id_6 partition 0
            //id_7 partition 2
            //id_8 partition 1
            //id_9 partition 2

            // send data - asynchronous
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
            }).get(); //block the .send() to make it synchronous - don`t do this in production
        }

        //flush data
        producer.flush();

        //flush and close producer
        producer.close();
    }
}
