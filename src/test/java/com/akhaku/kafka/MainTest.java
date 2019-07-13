package com.akhaku.kafka;

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
    public void testRunProducer() {
        KafkaProducer<String, String> producer = new KafkaProducerFactory().create();
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "hello world " + i);
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

    private static String toString(RecordMetadata recordMetadata) {
        return new StringBuilder("RecordMetadata{topic=").append(recordMetadata.topic())
            .append(",parition=").append(recordMetadata.partition())
            .append(",offset=").append(recordMetadata.offset())
            .append(",timestamp=").append(recordMetadata.timestamp())
            .append("}").toString();
    }
}
