package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
import java.time.format.DateTimeFormatter;
import java.util.Iterator;

/**
 * In this class the JFK airport trips program has to be implemented.
 */
public class JFKAlarms {

    private final static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        env.readTextFile(params.get("input"))
                .filter((FilterFunction<String>) s -> {
                    String[] sp = s.split(",");
                    return sp[5].equals("2") && Long.parseLong(sp[3]) > 1;
                })
                .map(in -> {
                    String[] fieldArray = in.split(",");
                    return new Tuple4<>(Long.parseLong(fieldArray[0]),
                            fieldArray[1],
                            fieldArray[2],
                            Long.parseLong(fieldArray[3]));
                })
                .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Long, String, String, Long>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple4<Long, String, String, Long> tuple) {
                        return LocalDateTime.parse(tuple.f1, dateTimeFormatter).atZone(ZoneId.of("CET"))
                                .toInstant().toEpochMilli();
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .reduce((t1, t2) -> {
                    t1.setField(t2.f2, 2);
                    t1.setField(t1.f3 + t2.f3, 3);
                    return t1;
                })
                .writeAsCsv(params.get("output").concat("/jfkAlarms.csv"), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute("JFKAlarms");
    }
}
