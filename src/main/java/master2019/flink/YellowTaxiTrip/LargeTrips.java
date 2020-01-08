package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * In this class the Large trips program has to be implemented
 */
public class LargeTrips {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<String> inputText = env.readTextFile(params.get("input"));

        env.getConfig().setGlobalJobParameters(params);

        SingleOutputStreamOperator<Tuple4<Long, String, String, Long>> filterStream = inputText
                .map(new MapFunction<String, Tuple4<Long, String, String, Long>>() {
                    public Tuple4<Long, String, String, Long> map(String in) throws ParseException {
                        String[] fieldArray = in.split(",");
                        Tuple4<Long, String, String, Long> out = new Tuple4(Long.parseLong(fieldArray[0]),
                                fieldArray[1], fieldArray[2], stringDatetoSeconds(fieldArray[1], fieldArray[2]));
                        return out;
                    }
                });

        if (params.has("output")) {
            filterStream.writeAsText(params.get("output"));
        }

        env.execute("LargeTrips");
    }

    private static Long stringDatetoSeconds(String start, String finish) throws ParseException {

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Long startSeconds = dateFormat.parse(start).getTime() / 1000;
        Long finishSeconds = dateFormat.parse(finish).getTime() / 1000;
        return finishSeconds - startSeconds;
    }
}
