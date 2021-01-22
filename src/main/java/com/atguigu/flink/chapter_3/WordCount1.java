package com.atguigu.flink.chapter_3;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class WordCount1 {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.读取数据
        DataSource<String> inputDS = env.readTextFile("input/word.txt");
        //3.处理数据
        FlatMapOperator<String, String> wordDS = inputDS.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(word);
                        }
                    }
                }
        );
        MapOperator<String, Tuple2<String, Long>> wordAndOneDS = wordDS.map(
                new MapFunction<String, Tuple2<String, Long>>() {

                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        return Tuple2.of(value, 1L);
                    }
                });
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneDS.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> resultDS = wordAndOneGroup.sum(1);
        //4.输出
        resultDS.print();
        //5.执行（批处理不需要）
    }
}
