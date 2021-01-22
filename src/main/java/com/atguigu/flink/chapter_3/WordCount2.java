package com.atguigu.flink.chapter_3;

import jdk.nashorn.internal.codegen.types.Type;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class WordCount2 {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.读取数据
        DataSource<String> inputDS = env.readTextFile("input/word.txt");
        //3.处理数据
        FlatMapOperator<String, String> wordDS = inputDS.flatMap(
                (String value, Collector<String> out) -> {
                    String[] words = value.split(" ");
                    for (String word : words) {
                        out.collect(word);
                    }
                }).returns(Types.STRING);
        MapOperator<String, Tuple2<String, Long>> wordAndOneDS = wordDS
                .map(value -> Tuple2.of(value, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneDS.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> resultDS = wordAndOneGroup.sum(1);
        //4.输出
        resultDS.print();
        //5.执行（批处理不需要）
    }
}
