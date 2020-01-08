package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * In this class the JFK airport trips program has to be implemented.
 */
public class JFKAlarms {
    public static void main(String[] args){

        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<String> inputText = env.readTextFile(params.get("input"));

        env.getConfig().setGlobalJobParameters(params);


    }
}
