package com.akhaku.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

public class MainTest {
    private static final String TOPIC = "first_topic";
    @Test
    public void testRunProducer() {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "hello world");
        KafkaProducer<String, String> producer = new KafkaProducerFactory().create();
        producer.send(record);
        producer.flush();
        producer.close();
    }
}
