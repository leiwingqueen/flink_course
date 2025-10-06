package com.example.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AverageProfit {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> data = env.readTextFile("./average_profit/avg");

        // TODO: implement average profit per month
        // 1. map function: month, product, category, profit, count
        //        [June,Category4,Perfume,10,1]
        DataStream<OutputData> output = data.map(new MapFunction<String, RawData>() {
            @Override
            public RawData map(String s) throws Exception {
                String[] items = s.split(",");
                return new RawData(items[1], Integer.parseInt(items[4]), 1);
            }
        }).keyBy(new KeySelector<RawData, String>() {
            @Override
            public String getKey(RawData rawData) throws Exception {
                return rawData.month;
            }
        }).reduce(new ReduceFunction<RawData>() {
            @Override
            public RawData reduce(RawData current, RawData pre) throws Exception {
                return new RawData(current.month, pre.profit + current.profit, current.count + pre.count);
            }
        }).map(new MapFunction<RawData, OutputData>() {
            @Override
            public OutputData map(RawData rawData) throws Exception {
                return new OutputData(rawData.month,((double)rawData.profit)/rawData.count);
            }
        });


        // 2. groupBy 'month'. use reduce function to sum profit and count

        // June { [Category5,Bat,12,1] Category4,Perfume,10,1}	//rolling reduce
        // reduced = { [Category4,Perfume,22,2] ..... }
        // month, avg. profit
        // 3. map function: month, avg. profit

        //profitPerMonth.print();
        output.writeAsText("./average_profit/output");

        // execute program
        env.execute("Avg Profit Per Month");
    }

    // 01-06-2018,June,Category5,Bat,12
    public static class RawData{
        private String month;
        private int profit;
        private int count;

        public RawData(String month, int profit, int count) {
            this.month = month;
            this.profit = profit;
            this.count = count;
        }
    }

    public static class OutputData{
        private String month;
        private double avgProfit;

        public OutputData(String month, double avgProfit) {
            this.month = month;
            this.avgProfit = avgProfit;
        }

        @Override
        public String toString() {
            return this.month+","+this.avgProfit;
        }
    }
}