package com.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class CountWords {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        DataStream<String> dataStream = StreamUtil.getDataStream(env, params);

        if (dataStream == null) {
            System.exit(1);
            return;
        }

        DataStream<Tuple2<String, Integer>> wordCountStream = dataStream
                .flatMap(new WordCount())
                .keyBy(0)
                .sum(1);
        wordCountStream.print();

        env.execute("Count");
    }

    public static class WordCount implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word : sentence.split(" ")) {
                collector.collect(new Tuple2<String, Integer>(word, 1));

            }

        }
    }

}
