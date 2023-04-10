package com.example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RoundUp {

    // Defining the main method
    public static void main(String[] args) throws Exception {
        // Creating a Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Creating a data stream from a socket source and applying a filter operation to it
        DataStream<Long> dataStream = env.socketTextStream("localhost", 9999)
                .filter(new Filter())
                .map(new Round());

        // Printing the resulting filtered data stream to the console
        dataStream.print();

        // Starting the Flink execution environment
        env.execute("RoundUp");
    }

    public static class Round implements MapFunction<String, Long> {
        @Override
        public Long map(String s) throws Exception {
            double d = Double.parseDouble(s.trim());
            return Math.round(d);
        }
    }

    public static class Filter implements FilterFunction<String> {

        @Override
        public boolean filter(String s) throws Exception {
            try {
                Double.parseDouble(s.trim());
                return true;
            } catch (Exception e) {
            }

            return false;
        }
    }
}
