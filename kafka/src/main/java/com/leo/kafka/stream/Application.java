package com.leo.kafka.stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class Application {
    public static void main(String[] args) {

        //1 定义输入topic
        String from = "first";
        //2 定义输出topic
        String to = "second";

        // 设置参数
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFile");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");

        StreamsConfig config = new StreamsConfig(props);

        //构建拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("SOURCE", from)
                .addProcessor("PROCESS", new ProcessorSupplier() {
                    @Override
                    public Processor get() {
                        return new LogProcessor();
                    }
                }, "SOURCE")
                .addSink("SINK", to, "PROCESS");

        //创建kafka stream
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }
}
