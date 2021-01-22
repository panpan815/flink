package com.atguigu.flink.chapter_5.transform;

import com.atguigu.flink.chapter_5.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink02_map_rich {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> sensorDS = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<WaterSensor> resultDS = sensorDS.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");
                return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
            }
        });
        resultDS.print();
        env.execute();
    }
public static class MyMapRichFunction extends RichMapFunction<String,WaterSensor>{
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("open....");
    }
    @Override
    public WaterSensor map(String s) throws Exception {
        String[] split = s.split(",");
        return new WaterSensor(getRuntimeContext().getTaskName()+"------"+getRuntimeContext().getTaskNameWithSubtasks(),Long.valueOf(split[1]), Integer.valueOf(split[2]));
    }
    @Override
    public void close() throws Exception {

    }


}

}
