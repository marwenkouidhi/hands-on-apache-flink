package com.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class AverageViews {

    public static void main(String[] args) throws Exception {
        final String taskName = "AverageViews";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        DataStream<String> dataStream = StreamUtil.getDataStream(env, params, taskName);

        if (dataStream == null) {
            System.exit(1);
            return;
        }

        DataStream<Tuple2<String, Double>> averageDataStream = dataStream
                .map(new RowSplitter())
                .keyBy(0)
                .reduce(new SumAndCount())
                .map(new Average());


        averageDataStream.print();

        env.execute(taskName);
    }

    public static class Average implements MapFunction<Tuple3<String, Double, Integer>, Tuple2<String, Double>> {


        @Override
        public Tuple2<String, Double> map(Tuple3<String, Double, Integer> input) throws Exception {
            return new Tuple2<String, Double>(
                    input.f0,
                    input.f1 / input.f2
            );
        }
    }

    public static class SumAndCount implements ReduceFunction<Tuple3<String, Double, Integer>> {

        @Override
        public Tuple3<String, Double, Integer> reduce(Tuple3<String, Double, Integer> cumulative, Tuple3<String, Double, Integer> input) throws Exception {
            return new Tuple3<String, Double, Integer>(
                    input.f0,
                    input.f1 + cumulative.f1,
                    input.f2 + cumulative.f2
            );
        }
    }

    public static class RowSplitter implements MapFunction<String, Tuple3<String, Double, Integer>> {

        @Override
        public Tuple3<String, Double, Integer> map(String row) throws Exception {
            String[] fields = row.split(",");
            if (fields.length == 2) {
                return new Tuple3<String, Double, Integer>(
                        fields[0],   /* webpage id*/
                        Double.parseDouble(fields[1]), /* view time in minutes */
                        1 /* count */
                );
            }
            return null;
        }
    }
}
