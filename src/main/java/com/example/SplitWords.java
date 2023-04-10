package com.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SplitWords {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        DataStream<String> dataStream;

        if (params.has("input")) {
            System.out.println("Executing SplitWords job with file input");
            dataStream = env.readTextFile(params.get("input"));
        } else if (params.has("host") && params.has("port")) {
            System.out.println("Executing SplitWords job with socket stream");
            dataStream = env.socketTextStream(params.get("host"), Integer.parseInt(params.get("port")));
        } else {
            System.out.println("Use --host and --port to execute the job with socket stream");
            System.out.println("Use --input to execute the job with an input file");
            System.exit(1);
            return;
        }

        DataStream<String> wordDataStream = dataStream.flatMap(new Splitter());
        wordDataStream.print();
        env.execute("SplitWords");
    }

    public static class Splitter implements FlatMapFunction<String, String> {

        @Override
        public void flatMap(String sentence, Collector<String> collector) throws Exception {
            for (String word : sentence.split(" ")) {
                collector.collect(word);
            }
        }
    }
}
