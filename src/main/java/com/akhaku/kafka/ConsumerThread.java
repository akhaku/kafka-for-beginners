package com.akhaku.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerThread implements Runnable, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerThread.class.getName());

    private final CountDownLatch latch;
    private final KafkaConsumer<String, String> consumer;

    public ConsumerThread(CountDownLatch latch, String topic) {
        this.latch = latch;
        this.consumer = KafkaConsumerFactory.create();
        consumer.subscribe(Collections.singleton(topic));
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.info("Key: " + record.key() + ", Value: " + record.value());
                    LOGGER.info("Paritition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (WakeupException e)  {
            LOGGER.info("Received shutdown signal");
        } finally {
            consumer.close();
            latch.countDown(); // tell main code we're done
        }
    }

    @Override
    public void close() {
        LOGGER.info("Waking up consumer");
        // interrupts consumer polling, throws WakeUpException
        consumer.wakeup();
    }
}
