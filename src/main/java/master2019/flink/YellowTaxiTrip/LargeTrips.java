package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * In this class the Large trips program has to be implemented
 */
public class LargeTrips {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final DataStream<String> inputText = env.readTextFile(params.get("input"));

        env.getConfig().setGlobalJobParameters(params);

        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        final Long minTime = (19L*60) + 59;

        SingleOutputStreamOperator<Tuple4<Long, Date, Date, Long>> filterStream = inputText
                .map(new MapFunction<String, Tuple4<Long, Date, Date, Long>>() {
                    public Tuple4<Long, Date, Date, Long> map(String in) throws ParseException {
                        String[] fieldArray = in.split(",");
                        Tuple4<Long, Date, Date, Long> out = new Tuple4(Long.parseLong(fieldArray[0]),
                                dateFormat.parse(fieldArray[1]), dateFormat.parse(fieldArray[2]),
                                stringDatetoSeconds(fieldArray[1], fieldArray[2]));
                        return out;
                    }
                })
                .filter(new FilterFunction<Tuple4<Long, Date, Date, Long>>() {
                    public boolean filter(Tuple4<Long, Date, Date, Long> mapTuple) throws Exception {
                        return(mapTuple.f3 > minTime);
                    }
                });

        KeyedStream<Tuple4<Long, Date, Date, Long>, Tuple> keyedStream = filterStream.
                assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Long, Date, Date, Long>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple4<Long, Date, Date, Long> tuple) {
                        return tuple.f1.getTime();
                    }
                })
                .keyBy(0);

        keyedStream.window(TumblingEventTimeWindows.of(Time.hours(3))).sum(3);

        if (params.has("output")) {
            filterStream.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);
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
