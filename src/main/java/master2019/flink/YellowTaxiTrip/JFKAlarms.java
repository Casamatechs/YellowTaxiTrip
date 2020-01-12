package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * In this class the JFK airport trips program has to be implemented.
 */
public class JFKAlarms {

    /**
     * Formatter to convert from the csv date format to Java8 DateTime object. This will be used only to assign
     * timestamps to the tuples so we avoid the usage of big objects during the execution.
     */
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        env.readTextFile(params.get("input"))
                /**
                 * First of all we apply a filter before building the Flink tuples. Doing this we only need the final
                 * required values when composing the tuples, so we can avoid the creation of thousands of unnecessary
                 * tuples.
                 */
                .filter((FilterFunction<String>) s -> {
                    String[] sp = s.split(",");
                    return sp[5].equals("2") && Long.parseLong(sp[3]) > 1;
                })
                /**
                 * Now we only have the valid tuples to be processed. The map will compose a tuples of 4 elements,
                 * without parsing the dates to Java DataTime objects, because these are heavier than a String and
                 * are not necessary for anything but assigning the timestamps, which is done afterwards.
                 */
                .map(in -> {
                    String[] fieldArray = in.split(",");
                    return new Tuple4<>(Long.parseLong(fieldArray[0]),
                            fieldArray[1],
                            fieldArray[2],
                            Long.parseLong(fieldArray[3]));
                })
                /**
                 * Because we are using Java8 lambda expressions, we need to specify to Flink the types of the tuples.
                 * Otherwise, it'd use Java Object serialization, which is extremely inefficient.
                 */
                .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.STRING, Types.LONG))
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
                 * We define a tumbling window (size and slide are the same) of 1 hour.
                 */
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                /**
                 * Because the input tuple has the output format, we decided to use a reduce function which only
                 * aggregates the number of passengers. To avoid the creation of unnecessary objects, we reused the
                 * initial window tuple and updated the required fields.
                 */
                .reduce((t1, t2) -> {
                    t1.setField(t2.f2, 2); // We update the date to get at the end the time the last trip finishes.
                    t1.setField(t1.f3 + t2.f3, 3); // We sum the passengers to get at the end the total sum.
                    return t1;
                })
                .writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute("JFKAlarms");
    }
}
