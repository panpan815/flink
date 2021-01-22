package com.atguigu.flink.chapter_5.resource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Source_kafka {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.setProperty("group.id","source_kafka");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer<String> sensor = new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), properties);
        DataStreamSource<String> kafkaDS = env.addSource(sensor);
        kafkaDS.print();
        env.execute();
    }
}
