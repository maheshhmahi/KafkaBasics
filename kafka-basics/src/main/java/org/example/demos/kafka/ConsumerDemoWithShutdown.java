package org.example.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class);

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer");

        String topic = "firstTopic";

        // for local
//        KafkaConsumer<String, String> kafkaConsumer = getLocalKafkaConsumer();

        // for remote (upstash)
        KafkaConsumer<String, String> kafkaConsumer = getUpstashKafkaConsumer();

        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, Let's exit by calling consumer.wakeup()....");
                kafkaConsumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
            // subscribe consumer to our topic(s)
            kafkaConsumer.subscribe(Arrays.asList(topic));

            //poll for new data
            while (true) {

//                log.info("polling");

                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Wake up excepiton!");
            // we ignore this as this is an expected exception when closing a consumer
        } catch (Exception e) {
            log.error("Unexpected exception", e);
        } finally {
            kafkaConsumer.close();
            log.info("The consumer is now gracefully closed");
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
