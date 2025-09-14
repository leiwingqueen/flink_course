package com.example.flink;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
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
        // TODO: implement here


        // join datasets on person_id
        // joined format will be <id, person_name, state>
        DataSet<Tuple3<Integer, String, String>> joined;

        joined.writeAsCsv(params.get("output"), "\n", " ");

        env.execute("Join example");
    }
}
