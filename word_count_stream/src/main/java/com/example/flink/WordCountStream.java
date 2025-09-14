package com.example.flink;//package p1;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCountStream {
    public static void main(String[] args)
            throws Exception {
        // use stream environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.setParallelism(1);

        ParameterTool params = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(params);

        DataStreamSource<String> text = env.socketTextStream("localhost", 9999);

        // hint:
        // 1. filter the data set to keep only words that start with "N"
        // 2. use the Tokenizer class to map the filtered data set to (word, 1) tuples
        // 3. group by the first tuple field and sum up the second tuple field
        // Your code here
        SingleOutputStreamOperator<Tuple2<String, Integer>> counts = text.filter((s) -> s.startsWith("N"))
                .map(s -> new Tuple2<String, Integer>(s, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT)) // 需要显示指定类型
                .keyBy(tuple -> tuple.f0).sum(1);
        counts.print();
        env.execute("WordCount Stream Example");
    }

    public static final class Tokenizer
            implements MapFunction<String, Tuple2<String, Integer>> {
        public Tuple2<String,
                Integer> map(String value) {
            return new Tuple2<String, Integer>(value, Integer.valueOf(1));
        }
    }
}