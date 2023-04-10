// Importing necessary classes from Flink API

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// Defining the FilterStrings class
public class FilterStrings {

    // Defining the main method
    public static void main(String[] args) throws Exception {

        // Creating a Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Creating a data stream from a socket source and applying a filter operation to it
        DataStream<String> dataStream = env.socketTextStream("localhost", 9999).filter(new Filter());

        // Printing the resulting filtered data stream to the console
        dataStream.print();

        // Starting the Flink execution environment
        env.execute("FilterStrings");
    }

    // Defining a nested class Filter that implements the FilterFunction interface for String type
    public static class Filter implements FilterFunction<String> {

        // Overriding the filter method of the FilterFunction interface
        @Override
        public boolean filter(String s) throws Exception {
            try {
                // Trying to parse the incoming string as a Double and trim it
                Double.parseDouble((s.trim()));
                // If parsing succeeds, return true (i.e., the string is a number)
                return true;
            } catch (Exception e) {
            }

            // If parsing fails, return false (i.e., the string is not a number)
            return false;
        }
    }
}
