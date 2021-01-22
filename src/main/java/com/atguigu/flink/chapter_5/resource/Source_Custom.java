package com.atguigu.flink.chapter_5.resource;

import com.atguigu.flink.chapter_5.WaterSensor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
import java.util.Random;

public class Source_Custom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDS = env.addSource(new MySource());
        sensorDS.print();
        env.execute();
    }

    public static class MySource implements SourceFunction<WaterSensor> {
        private volatile boolean isRuning = true;

        @Override
        public void run(SourceContext<WaterSensor> sourceContext) throws Exception {
            Random random = new Random();
            while (isRuning) {
                sourceContext.collect(
                        new WaterSensor(
                                "sensor_" + random.nextInt(3),
                                System.currentTimeMillis(),
                                random.nextInt(10)
                        )
                );
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            isRuning = false;

        }
    }
}
