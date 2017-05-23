package com.kafka.perf;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author Vivek Mittal
 */
public class KafkaPublisher {
    private static final int TOTAL_MESSAGES = 40_000_000;
    private static final int BATCH_SIZE = 10_000;
    private static final String HDD_KAFKA = "broker-list";
    private static final String SSD_KAFKA = "broker-list";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final String topic = args[0];

        String cluster = HDD_KAFKA;
        if (args.length == 2 && args[1].equalsIgnoreCase("SSD")) {
            cluster = SSD_KAFKA;
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", cluster);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        String[] namespaces = new String[]{"available", "live", "offer", "s_t", "state"};

        final String lstId = "LSTACCEP6Q38VYJE68SZS9KME";

        final int start = 1;

        for (int i = start; i < TOTAL_MESSAGES; i += BATCH_SIZE) {
            doit(topic, producer, namespaces, lstId, i, i + 10_000);
        }

        producer.close();
    }

    private static void doit(String topic, Producer kafkaPublisher, String[] namespaces, String lstId, int start, int end) throws InterruptedException, ExecutionException {
        List<Future> futures = new ArrayList<Future>();
        for (int i = start; i < end; i++) {
            for (String namespace : namespaces) {
                String listingid = lstId + i;
                final String key = listingid + namespace;
                final String msg = listingid+","+namespace+",pg,"+10;
                try {
                    futures.add(kafkaPublisher.send(new ProducerRecord<>(topic, key, msg)));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        System.out.println("Waiting for the threads to finish");
        for (Future future : futures) {
            future.get();
        }
    }

}
