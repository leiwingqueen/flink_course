package com.example.flink;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

@SuppressWarnings("serial")
public class InnerJoin {
    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataSource<String> ds1 = env.readTextFile(params.get("input1"));
        DataSource<String> ds2 = env.readTextFile(params.get("input2"));
        // hint:
        // input1 = path to person.csv
        // input2 = path to location.csv
        // Read person file and generate tuples out of each string read
        // person file format: person_id, person_name
        // location file format: person_id, state
        // use join,where,equalTo,with function to join two dataset
        // output person_id,person_name,state
        // implement here
        MapOperator<String, Tuple2<Integer, String>> mapOp1 = ds1.map((MapFunction<String, Tuple2<Integer, String>>) s -> {
            String[] arr = s.split(",");
            if (arr.length < 2) {
                throw new IllegalArgumentException("param error");
            }
            return new Tuple2<>(Integer.parseInt(arr[0]), arr[1]);
        }).returns(Types.TUPLE(Types.INT, Types.STRING));
        MapOperator<String, Tuple2<Integer, String>> mapOp2 = ds2.map((MapFunction<String, Tuple2<Integer, String>>) s -> {
            String[] arr = s.split(",");
            if (arr.length < 2) {
                throw new IllegalArgumentException("param error");
            }
            return new Tuple2<>(Integer.parseInt(arr[0]), arr[1]);
        }).returns(Types.TUPLE(Types.INT, Types.STRING));
        DataSet<Tuple3<Integer, String, String>> joined = mapOp1.join(mapOp2).where(0).equalTo(0).with((JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>) (p1, p2) -> new Tuple3<>(p1.f0, p1.f1, p2.f1)).returns(Types.TUPLE(Types.INT, Types.STRING, Types.STRING));
        // join datasets on person_id
        // joined format will be <id, person_name, state>

        joined.writeAsCsv(params.get("output"), "\n", " ");

        env.execute("Join example");
    }
}
