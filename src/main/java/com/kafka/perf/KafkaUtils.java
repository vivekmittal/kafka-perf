package com.kafka.perf;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

/**
 * @author Vivek Mittal
 */
public class KafkaUtils {
    public static List<PartitionInfo> getPartitionInfo(String brokers, String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        return consumer.partitionsFor(topic);
    }

    public static int numberOfPartitions(String brokers, String topic) {
        return getPartitionInfo(brokers, topic).size();
    }

}
