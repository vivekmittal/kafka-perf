package com.kafka.perf;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Vivek Mittal
 */
public class FlashConsumer {
    private static final ExecutorService executorService = Executors.newCachedThreadPool();

    private static final String HDD_KAFKA = "broker-list";
    private static final String SSD_KAFKA = "broker-list";
    private static AtomicInteger consumed = new AtomicInteger(0);

    public static void main(String[] args) throws InterruptedException {
        final String topic = args[0];

        String cluster = HDD_KAFKA;
        if (args.length == 2 && args[1].equalsIgnoreCase("SSD")) {
            cluster = SSD_KAFKA;
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", cluster);
        // props.put("group.id", args[1]);
        props.put("enable.auto.commit", "false");
        // props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");


        List<KafkaConsumer<String, String>> consumers = new ArrayList<>();

        for (PartitionInfo partitionInfo : KafkaUtils.getPartitionInfo(cluster, topic)) {
            TopicPartition partition = new TopicPartition(topic, partitionInfo.partition());

            final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.assign(Collections.singletonList(partition));
            consumers.add(consumer);
        }

        for (KafkaConsumer<String, String> consumer : consumers) {
            executorService.submit(() -> {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    consumed.addAndGet(records.count());
                }
            });
        }

        long timeinmillis = System.currentTimeMillis();
        while (true) {
            final double timeTaken = (System.currentTimeMillis() - timeinmillis) / 1000.0;
            final int consumedTotal = consumed.get();
            System.out.println("Consumed Count: " + consumedTotal + ", Time: " + timeTaken + ", Throughput: " + (double) consumedTotal / timeTaken);
            Thread.sleep(1000);
        }
    }
}
