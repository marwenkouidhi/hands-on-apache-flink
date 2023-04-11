package com.example;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamUtil {

    public static DataStream getDataStream(StreamExecutionEnvironment env, final ParameterTool params, String taskName) {
        env.getConfig().setGlobalJobParameters(params);
        DataStream<String> dataStream = null;

        if (params.has("input")) {
            System.out.println("Executing " + taskName + " task with file input");
            dataStream = env.readTextFile(params.get("input"));
        } else if (params.has("host") && params.has("port")) {
            System.out.println("Executing " + taskName + " task with socket stream");
            dataStream = env.socketTextStream(params.get("host"), Integer.parseInt(params.get("port")));
        } else {
            System.out.println("Use --host and --port to execute the job with socket stream");
            System.out.println("Use --input to execute the job with an input file");
        }

        return dataStream;
    }
}
