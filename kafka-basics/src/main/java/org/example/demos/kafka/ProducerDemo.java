package org.example.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

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
        kafkaProducer.send(producerRecord);
        
        // flush data - synchronous
        kafkaProducer.flush();
        
        // flush and close producer
        kafkaProducer.close();

        /*
            The data produced by a producer is asynchronous. Therefore, two additional functions,
            i.e., flush() and close() are required to ensure the producer is shut down after the message is sent to Kafka.

            The flush() will force all the data that was in .send() to be produced and close() stops the producer.
            If these functions are not executed, the data will never be sent to Kafka as the main Java thread will
            exit before the data are flushed.
        */
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
