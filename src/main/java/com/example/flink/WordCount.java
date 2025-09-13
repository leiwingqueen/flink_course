package com.example.flink;//package p1;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

public class WordCount {
    public static void main(String[] args)
            throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // env.setParallelism(1);

        ParameterTool params = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> text = env.readTextFile(params.get("input"));

        // hint:
        // 1. filter the data set to keep only words that start with "N"
        // 2. use the Tokenizer class to map the filtered data set to (word, 1) tuples
        // 3. group by the first tuple field and sum up the second tuple field
        // TODO: Your code here

        DataSet<Tuple2<String, Integer>> counts;
        if (params.has("output")) {
            counts.writeAsCsv(params.get("output"), "\n", " ");

            env.execute("WordCount Example");
        }
    }

    public static final class Tokenizer
            implements MapFunction<String, Tuple2<String, Integer>> {
        public Tuple2<String,
                Integer> map(String value) {
            return new Tuple2<String, Integer>(value, Integer.valueOf(1));
        }
    }
}