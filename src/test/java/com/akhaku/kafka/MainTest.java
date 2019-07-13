package com.akhaku.kafka;

import java.time.Duration;
import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainTest {
    private static final String TOPIC = "first_topic";
    private static final Logger LOGGER = LoggerFactory.getLogger(MainTest.class);

    @Test
    public void testRunProducer() throws Exception {
        KafkaProducer<String, String> producer = new KafkaProducerFactory().create();
        for (int i = 0; i < 10; i++) {
            String value = "hello world " + i;
            String key = "id_" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);
            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    LOGGER.info("Received new metadata: {}", toString(recordMetadata));
                } else {
                    LOGGER.error("Error producing message", e);
                }
            });
        }
        producer.flush();
        producer.close();
    }

    @Test
    public void testConsumer() throws Exception {
        KafkaConsumer<String, String> consumer = KafkaConsumerFactory.create();
        consumer.subscribe(Collections.singleton(TOPIC));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                LOGGER.info("Key: " + record.key() + ", Value: " + record.value());
                LOGGER.info("Paritition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
    }

    private static String toString(RecordMetadata recordMetadata) {
        return new StringBuilder("RecordMetadata{topic=").append(recordMetadata.topic())
            .append(",parition=").append(recordMetadata.partition())
            .append(",offset=").append(recordMetadata.offset())
            .append(",timestamp=").append(recordMetadata.timestamp())
            .append("}").toString();
    }
}
