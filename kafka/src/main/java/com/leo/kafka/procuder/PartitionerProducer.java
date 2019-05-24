package com.leo.kafka.procuder;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class PartitionerProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "hadoop103:9092");
        // 等待所有副本节点的应答
        props.put("acks", "all");
        // 消息发送最大尝试次数
        props.put("retries", 0);
        // 一批消息处理大小
        props.put("batch.size", 16384);
        // 增加服务端请求延时
        props.put("linger.ms", 1);
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        // key序列化
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 自定义分区
        props.put("partitioner.class", "com.leo.kafka.procuder.NewCustomPartitioner"); //新API
//        props.put("partitioner.class", "com.leo.kafka.procuder.OldCustomPartitioner"); //老API
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        producer.send(new ProducerRecord<String, String>("second", "2", "leo"), new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (metadata != null) {
                    System.out.println(metadata.partition());
                }
            }
        });
        producer.close();
    }
}
