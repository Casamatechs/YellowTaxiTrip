package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.stream.StreamSupport;

/**
 * In this class the Large trips program has to be implemented
 */
public class LargeTrips {

    final static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    final static DateTimeFormatter dayFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        final Long minTime = (19L * 60) + 59;

        env.readTextFile(params.get("input"))
                .map(in -> {
                    String[] fieldArray = in.split(",");
                    return new Tuple4<>(Long.parseLong(fieldArray[0]),
                            LocalDateTime.parse(fieldArray[1], dateTimeFormatter), LocalDateTime.parse(fieldArray[2], dateTimeFormatter),
                            stringDatetoSeconds(fieldArray[1], fieldArray[2]));
                })
                .returns(Types.TUPLE(Types.LONG, Types.LOCAL_DATE_TIME, Types.LOCAL_DATE_TIME, Types.LONG))
                .filter(mapTuple -> (mapTuple.f3 > minTime))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Long, LocalDateTime, LocalDateTime, Long>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple4<Long, LocalDateTime, LocalDateTime, Long> tuple) {
                        return tuple.f1.toInstant(ZoneId.of("CET").getRules().getOffset(tuple.f1)).toEpochMilli();
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.hours(3)))
                .apply(new LargeTripsCounter())
                .writeAsCsv(params.get("output").concat("/largeTrips.csv"), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute("LargeTrips");
    }

    private static Long stringDatetoSeconds(String start, String finish) {

        Long startSeconds = LocalDateTime.parse(start, dateTimeFormatter).toEpochSecond(ZoneOffset.UTC);
        Long finishSeconds = LocalDateTime.parse(finish, dateTimeFormatter).toEpochSecond(ZoneOffset.UTC);
        return finishSeconds - startSeconds;
    }

    private static class LargeTripsCounter implements WindowFunction<Tuple4<Long, LocalDateTime, LocalDateTime, Long>, Tuple5<Long, String, Long, String, String>, Tuple, TimeWindow> {

        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple4<Long, LocalDateTime, LocalDateTime, Long>> input, Collector<Tuple5<Long, String, Long, String, String>> out) {
            final long numberRecords = input.spliterator().getExactSizeIfKnown();
            if (numberRecords > 4) {
                Tuple4<Long, LocalDateTime, LocalDateTime, Long> first = StreamSupport.stream(input.spliterator(), false).findFirst().get();
                out.collect(new Tuple5<>(first.f0, dayFormat.format(first.f1),
                        numberRecords, dateTimeFormatter.format(first.f1),
                        dateTimeFormatter.format(StreamSupport.stream(input.spliterator(), false)
                                .skip(numberRecords - 1).findFirst().get().f2)));
            }
        }
    }
}
