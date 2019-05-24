package com.leo.kafka.consumer.newCustomerApi;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class CustomConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("zookeeper.connect", "hadoop102:2181");
        props.put("group.id", "g1");
        props.put("zookeeper.session.timeout.ms", "500");
        props.put("zookeeper.sync.time.ms", "250");
        props.put("auto.commit.interval.ms", "1000");

        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        HashMap<String, Integer> topicCount = new HashMap<>();
        topicCount.put("first", 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCount);
        KafkaStream<byte[], byte[]> stream = consumerMap.get("first").get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            System.out.println(new String(it.next().message()));
        }
    }
}
