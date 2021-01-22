package com.atguigu.flink.chapter_5.resource;

import com.atguigu.flink.chapter_5.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class Source_collection {
    public static void main(String[] args) throws Exception {
        List<WaterSensor> waterSensors = Arrays.asList(
                new WaterSensor("ws_001", 1L, 12),
                new WaterSensor("ws_002", 2L, 13),
                new WaterSensor("ws_003", 3L, 14));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .fromCollection(waterSensors)
                .print();
        env.fromElements(1,2,3,4,4).print();

        env.execute();
    }
}
