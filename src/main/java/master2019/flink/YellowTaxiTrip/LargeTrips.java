package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.stream.StreamSupport;

/**
 * In this class the Large trips program has to be implemented
 */
public class LargeTrips {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final DataStream<String> inputText = env.readTextFile(params.get("input"));

        env.getConfig().setGlobalJobParameters(params);

        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        final Long minTime = (19L * 60) + 59;

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
                        return (mapTuple.f3 > minTime);
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

        SingleOutputStreamOperator out = keyedStream.window(TumblingEventTimeWindows.of(Time.hours(3))).apply(new LargeTripsCounter());

        if (params.has("output")) {
            out.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        }

        env.execute("LargeTrips");
    }

    private static Long stringDatetoSeconds(String start, String finish) throws ParseException {

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Long startSeconds = dateFormat.parse(start).getTime() / 1000;
        Long finishSeconds = dateFormat.parse(finish).getTime() / 1000;
        return finishSeconds - startSeconds;
    }

    private static class LargeTripsCounter implements WindowFunction<Tuple4<Long, Date, Date, Long>, Tuple5<Long, String, Long, String, String>, Tuple, TimeWindow> {

        final SimpleDateFormat dayFormat = new SimpleDateFormat("yyyy-MM-dd");
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple4<Long, Date, Date, Long>> input, Collector<Tuple5<Long, String, Long, String, String>> out) {
            final long numberRecords = StreamSupport.stream(input.spliterator(), false).count();
            if (numberRecords > 4) {
                Tuple4<Long, Date, Date, Long> first = StreamSupport.stream(input.spliterator(), false).findFirst().get();
                out.collect(new Tuple5<>(first.f0, dayFormat.format(first.f1),
                        numberRecords, dateFormat.format(first.f1),
                        dateFormat.format(StreamSupport.stream(input.spliterator(), false)
                                .skip(numberRecords-1).findFirst().get().f2)));
            }
        }
    }
}
