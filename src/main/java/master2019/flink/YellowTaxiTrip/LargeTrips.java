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

    /**
     * Formatter to convert from the csv date format to Java8 DateTime object.
     */
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        /**
         * The minimum time to go through the filter.
         */
        final Long minTime = (20L * 60) - 1;

        env.readTextFile(params.get("input"))
                /**
                 * First of all we compose the Flink tuples attaching as a field the number of seconds of the trip
                 * to be able to filter during the next step.
                 */
                .map(in -> {
                    String[] fieldArray = in.split(",");
                    return new Tuple4<>(Long.parseLong(fieldArray[0]),
                            fieldArray[1], fieldArray[2],
                            stringDatetoSeconds(fieldArray[1], fieldArray[2]));
                })
                /**
                 * Because we are using Java8 lambda expressions, we need to specify to Flink the types of the tuples.
                 * Otherwise, it'd use Java Object serialization, which is extremely inefficient.
                 */
                .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.STRING, Types.LONG))
                /**
                 * Now we apply a filter to remove the trips during less than 20 minutes.
                 */
                .filter(mapTuple -> (mapTuple.f3 > minTime))
                /**
                 * We assign the timestamps using the start time for each tuple. Since we are using Java8 API, we
                 * decided not to use the deprecated java.util.Date class. Instead, we used java.time.LocalDateTime
                 * which provides information about the time zone too. Because the data comes from New York, we used
                 * that time zone. On this assignment, it doesn't  matter because we don't have data from different
                 * time zones, but in real applications, this date management is really important.
                 */
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Long, String, String, Long>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple4<Long, String, String, Long> tuple) {
                        return LocalDateTime.parse(tuple.f1, dateTimeFormatter).atZone(ZoneId.of("America/New_York"))
                                .toInstant().toEpochMilli();
                    }
                })
                /**
                 * We group the tuples by VendorId
                 */
                .keyBy(0)
                /**
                 * We define a tumbling window (size and slide are the same) of 3 hours.
                 */
                .window(TumblingEventTimeWindows.of(Time.hours(3)))
                /**
                 * We apply a custom WindowFunction which implements the logic to produce the correct output.
                 */
                .apply(new LargeTripsCounter())
                .writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute("LargeTrips");
    }

    private static Long stringDatetoSeconds(String start, String finish) {

        Long startSeconds = LocalDateTime.parse(start, dateTimeFormatter).toEpochSecond(ZoneOffset.UTC);
        Long finishSeconds = LocalDateTime.parse(finish, dateTimeFormatter).toEpochSecond(ZoneOffset.UTC);
        return finishSeconds - startSeconds;
    }

    private static class LargeTripsCounter implements WindowFunction<Tuple4<Long, String, String, Long>,
            Tuple5<Long, String, Long, String, String>, Tuple, TimeWindow> {

        public void apply(Tuple tuple, TimeWindow timeWindow,
                          Iterable<Tuple4<Long, String, String, Long>> input,
                          Collector<Tuple5<Long, String, Long, String, String>> out) {
            /**
             * We get the number of tuples in the window in the first place to avoid unnecessary calculations.
             */
            final long numberRecords = input.spliterator().getExactSizeIfKnown();
            if (numberRecords > 4) {
                /**
                 * Because we are using Java8 API, we decided to use Java Streams, which optimize the use of iterators
                 * and allow us to skip unnecessary tuples.
                 */
                Tuple4<Long, String, String, Long> first = StreamSupport.stream(input.spliterator(), false)
                        .findFirst().get();
                out.collect(new Tuple5<>(first.f0, first.f1.substring(0, 10),
                        numberRecords, first.f1,
                        StreamSupport.stream(input.spliterator(), false)
                                .skip(numberRecords - 1).findFirst().get().f2));
            }
        }
    }
}
