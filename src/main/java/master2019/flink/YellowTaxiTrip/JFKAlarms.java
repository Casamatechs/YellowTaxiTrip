package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

/**
 * In this class the JFK airport trips program has to be implemented.
 */
public class JFKAlarms {
    public static void main(String[] args) throws Exception {

        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.getConfig().setGlobalJobParameters(params);

        env.readTextFile(params.get("input"))
                .map(new MapFunction<String, Tuple5<Long, Date, Date, Long, Long>>() {
                    public Tuple5<Long, Date, Date, Long, Long> map(String in) throws ParseException {
                        String[] fieldArray = in.split(",");
                        Tuple5<Long, Date, Date, Long, Long> out = new Tuple5(Long.parseLong(fieldArray[0]),
                                dateFormat.parse(fieldArray[1]), dateFormat.parse(fieldArray[2]),
                                Long.parseLong(fieldArray[3]), Long.parseLong(fieldArray[5]));
                        return out;
                    }
                })
                .filter(new FilterFunction<Tuple5<Long, Date, Date, Long, Long>>() {
                    public boolean filter(Tuple5<Long, Date, Date, Long, Long> mapTuple) throws Exception {
                        return (mapTuple.f4 == 2 && mapTuple.f3 > 1);
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple5<Long, Date, Date, Long, Long>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple5<Long, Date, Date, Long, Long> tuple) {
                        return tuple.f1.getTime();
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .apply(new PassengerCounter())
                .writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        /*SingleOutputStreamOperator<Tuple5<Long, Date, Date, Long, Long>> filterStream = inputText
                .map(new MapFunction<String, Tuple5<Long, Date, Date, Long, Long>>() {
                    public Tuple5<Long, Date, Date, Long, Long> map(String in) throws ParseException {
                        String[] fieldArray = in.split(",");
                        Tuple5<Long, Date, Date, Long, Long> out = new Tuple5(Long.parseLong(fieldArray[0]),
                                dateFormat.parse(fieldArray[1]), dateFormat.parse(fieldArray[2]),
                                Long.parseLong(fieldArray[3]), Long.parseLong(fieldArray[5]));
                        return out;
                    }
                })
                .filter(new FilterFunction<Tuple5<Long, Date, Date, Long, Long>>() {
                    public boolean filter(Tuple5<Long, Date, Date, Long, Long> mapTuple) throws Exception {
                        return(mapTuple.f4 == 2 && mapTuple.f3 > 1);
                    }
                });

        KeyedStream<Tuple5<Long, Date, Date, Long, Long>, Tuple> keyedStream = filterStream
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple5<Long, Date, Date, Long, Long>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple5<Long, Date, Date, Long, Long> tuple) {
                        return tuple.f1.getTime();
                    }
                })
                .keyBy(0);

        SingleOutputStreamOperator out = keyedStream.window(TumblingEventTimeWindows.of(Time.hours(1))).apply(new PassengerCounter());

        if (params.has("output")) {
            out.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        }*/

        env.execute("JFKAlarms");


    }

    private static class PassengerCounter implements WindowFunction<Tuple5<Long, Date, Date, Long, Long>, Tuple4<Long, String, String, Long>, Tuple, TimeWindow> {

        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple5<Long, Date, Date, Long, Long>> input, Collector<Tuple4<Long, String, String, Long>> out) throws Exception {
            Iterator<Tuple5<Long, Date, Date, Long, Long>> iterator = input.iterator();
            Tuple5<Long, Date, Date, Long, Long> first = iterator.next();
            Long id = -1L;
            String start = "";
            String end = "";
            Long counter = 0L;
            if (first != null) {
                id = first.f0;
                start = dateFormat.format(first.f1);
                end = dateFormat.format(first.f2);
                counter += first.f3;
            }
            while (iterator.hasNext()) {
                Tuple5<Long, Date, Date, Long, Long> next = iterator.next();
                end = dateFormat.format(next.f2);
                counter += next.f3;
            }
            out.collect(new Tuple4<Long, String, String, Long>(id, start, end, counter));
        }
    }
}
