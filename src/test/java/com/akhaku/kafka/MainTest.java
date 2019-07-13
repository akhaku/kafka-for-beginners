package com.akhaku.kafka;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
    }

    @Test
    public void testConsumerThreads() throws Exception {
        int numConsumers = 4;
        CountDownLatch latch = new CountDownLatch(numConsumers);
        List<ConsumerThread> runnables = IntStream.range(0, numConsumers)
            .mapToObj(i -> new ConsumerThread(latch, TOPIC))
            .collect(Collectors.toList());
        IntStream.range(0, numConsumers).forEach(i -> {
            new Thread(runnables.get(i), "ConsumerThread-" + i).start();
        });
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Caught shutdown hook");
            runnables.forEach(t -> t.close());
            try {
                latch.await();
            } catch (InterruptedException e) {
                LOGGER.error("Error", e);
            }
        }));
        try {
            latch.await();
        } catch (Exception e) {
            LOGGER.error("Application interrupted", e);
        } finally {
            LOGGER.info("Application exiting");
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
