package com.leo.kafka.procuder;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class OldProducer {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("metadata.broker.list", "hadoop102:9092");
        props.put("request.required.acks", "1");
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ProducerConfig config = new ProducerConfig(props);
        Producer<Integer, String> producer = new Producer<Integer, String>(config);

        KeyedMessage<Integer, String> message = new KeyedMessage<Integer, String>("first", "hello world");
        producer.send(message);
    }
}
