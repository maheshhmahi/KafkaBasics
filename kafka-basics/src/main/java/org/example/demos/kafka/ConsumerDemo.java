package org.example.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer");

        String topic = "firstTopic";

        // for local
//        KafkaConsumer<String, String> kafkaConsumer = getLocalKafkaConsumer();

        // for remote (upstash)
        KafkaConsumer<String, String> kafkaConsumer = getUpstashKafkaConsumer();

        // subscribe consumer to our topic(s)
        kafkaConsumer.subscribe(Arrays.asList(topic));

        //poll for new data

        while (true) {

            log.info("polling");

            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset:" + record.offset());
            }
        }

    }

    private static KafkaConsumer<String, String> getLocalKafkaConsumer() {
        String bootstrapServers = "localhost:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // create the producer
        return new KafkaConsumer<>(properties);
    }

    private static KafkaConsumer<String, String> getUpstashKafkaConsumer() {
        // create Producer properties

        String groupId = "firstJavaApplication";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "https://relaxing-dogfish-5113-us1-kafka.upstash.io:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"cmVsYXhpbmctZG9nZmlzaC01MTEzJNn09qJXuRSlSTUAPlgtoySITljnlmIIKKU\" password=\"MzE4Nzc0N2MtM2Y4MC00NjhiLTlhZDUtM2FlMjNkYjQ0Nzhk\";");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create the producer
        return new KafkaConsumer<>(properties);
    }
}
