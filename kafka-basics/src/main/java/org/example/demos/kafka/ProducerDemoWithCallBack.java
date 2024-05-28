package org.example.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

    public static void main(String[] args) {
        log.info("I am a Kafka Producer");

        // for local
//        KafkaProducer<String, String> kafkaProducer = getLocalKafkaProducer();

        // for remote (upstash)
        KafkaProducer<String, String> kafkaProducer = getUpstashKafkaProducer();

        // create a producer record - local
//        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello World!");

        // create a producer record - remote
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("firstTopic", "Hello World!");

        // send data - asynchronous
        kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null) {
                log.info("Received new metadata. \n" +
                        "Topic:" + recordMetadata.topic() + "\n" +
                        "Partition: " + recordMetadata.partition() + "\n" +
                        "Offset: " + recordMetadata.offset() + "\n" +
                        "Timestamp: " + recordMetadata.timestamp());
            } else {
                log.error("Error while producing", e);
            }
        });


        //other way
        /*kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    log.info("Received new metadata. \n" +
                            "Topic:" + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    log.error("Error while producing", e);
                }
            }
        });*/
        
        // flush data - synchronous
        kafkaProducer.flush();
        
        // flush and close producer
        kafkaProducer.close();
    }

    private static KafkaProducer<String, String> getLocalKafkaProducer() {
        String bootstrapServers = "localhost:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        return new KafkaProducer<>(properties);
    }

    private static KafkaProducer<String, String> getUpstashKafkaProducer() {
        // create Producer properties

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "https://relaxing-dogfish-5113-us1-kafka.upstash.io:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"cmVsYXhpbmctZG9nZmlzaC01MTEzJNn09qJXuRSlSTUAPlgtoySITljnlmIIKKU\" password=\"MzE4Nzc0N2MtM2Y4MC00NjhiLTlhZDUtM2FlMjNkYjQ0Nzhk\";");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        return new KafkaProducer<>(properties);
    }
}
