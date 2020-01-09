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
import java.util.Iterator;

/**
 * In this class the JFK airport trips program has to be implemented.
 */
public class JFKAlarms {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        env.readTextFile(params.get("input"))
                .map(in -> {
                    String[] fieldArray = in.split(",");
                    return new Tuple5<>(Long.parseLong(fieldArray[0]),
                            fieldArray[1],
                            fieldArray[2],
                            Long.parseLong(fieldArray[3]), Long.parseLong(fieldArray[5]));
                })
                .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.STRING, Types.LONG, Types.LONG))
                .filter(mapTuple -> mapTuple.f4 == 2 && mapTuple.f3 > 1)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple5<Long, String, String, Long, Long>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple5<Long, String, String, Long, Long> tuple) {

                        return LocalDateTime.parse(tuple.f1, LargeTrips.dateTimeFormatter).toInstant(ZoneId.of("CET").getRules().getOffset(LocalDateTime.now())).toEpochMilli();
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .apply(new PassengerCounter())
                .writeAsCsv(params.get("output").concat("/jfkAlarms.csv"), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute("JFKAlarms");


    }

    private static class PassengerCounter implements WindowFunction<Tuple5<Long, String, String, Long, Long>, Tuple4<Long, String, String, Long>, Tuple, TimeWindow> {

        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple5<Long, String, String, Long, Long>> input, Collector<Tuple4<Long, String, String, Long>> out) throws Exception {
            Iterator<Tuple5<Long, String, String, Long, Long>> iterator = input.iterator();
            Tuple5<Long, String, String, Long, Long> first = iterator.next();
            final Tuple4<Long, String, String, Long> retTuple = new Tuple4<>(-1L, "", "", 0L);
            if (first != null) {
                retTuple.setField(first.f0, 0);
                retTuple.setField(first.f1, 1);
                retTuple.setField(first.f2, 2);
                retTuple.setField(retTuple.f3 + first.f3, 3);
            }
            while (iterator.hasNext()) {
                Tuple5<Long, String, String, Long, Long> next = iterator.next();
                retTuple.setField(next.f2, 2);
                retTuple.setField(retTuple.f3 + next.f3, 3);
            }
            out.collect(retTuple);
        }
    }
}
