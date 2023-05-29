package window;

import entity.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Arrays;

public class WindowTest1 {

    public static void main(String[] args) throws Exception {

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. source：获取数据
        DataStream<SensorReading> dataStream = env.fromCollection(
                Arrays.asList(
                        new SensorReading("senson_1", 1547718199L, 32.4),
                        new SensorReading("senson_1", 1547718121L, 3.4),
                        new SensorReading("senson_1", 1547718123L, 2.5),
                        new SensorReading("senson_1", 1547718125L, 39.4)
                )
        );

        // 3. 分组
        KeyedStream<SensorReading, Object> keyedStream = dataStream.keyBy(SensorReading::getId);

        // 4. window
        // Tumbling time Window
        WindowedStream<SensorReading, Object, TimeWindow> tumblingEventTimeWindow = keyedStream.window(TumblingEventTimeWindows.of(Time.minutes(5)));
        WindowedStream<SensorReading, Object, TimeWindow> tumblingProcessingTimeWindow = keyedStream.window(TumblingProcessingTimeWindows.of(Time.minutes(5)));


        // Sliding time Window
        WindowedStream<SensorReading, Object, TimeWindow> slidingEventTimeWindow = keyedStream.window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(2)));
        WindowedStream<SensorReading, Object, TimeWindow> slidingProcessingTimeWindow = keyedStream.window(SlidingProcessingTimeWindows.of(Time.minutes(10), Time.minutes(2)));

        // Session Window
        WindowedStream<SensorReading, Object, TimeWindow> eventTimeSessionWindow = keyedStream.window(EventTimeSessionWindows.withGap(Time.minutes(10)));
        WindowedStream<SensorReading, Object, TimeWindow> processingTimeSessionWindow = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.minutes(10)));

        // tumbling count window
        WindowedStream<SensorReading, Object, GlobalWindow> tumblingCountWindow = keyedStream.countWindow(10);

        // sliding count window
        WindowedStream<SensorReading, Object, GlobalWindow> slidingCountWindow = keyedStream.countWindow(10, 2);



        // 5. 聚合
        SingleOutputStreamOperator<SensorReading> sum1 = tumblingEventTimeWindow.sum(1);
        SingleOutputStreamOperator<SensorReading> sum2 = tumblingProcessingTimeWindow.sum(1);
        SingleOutputStreamOperator<SensorReading> sum3 = slidingEventTimeWindow.sum(1);
        SingleOutputStreamOperator<SensorReading> sum4 = slidingProcessingTimeWindow.sum(1);
        SingleOutputStreamOperator<SensorReading> sum5 = eventTimeSessionWindow.sum(1);
        SingleOutputStreamOperator<SensorReading> sum6 = processingTimeSessionWindow.sum(1);

        SingleOutputStreamOperator<SensorReading> sum7 = tumblingCountWindow.sum(1);
        SingleOutputStreamOperator<SensorReading> sum8 = slidingCountWindow.sum(1);


        // 6. sink: 输出
        sum1.print();
        sum2.print();
        sum3.print();
        sum4.print();
        sum5.print();
        sum6.print();
        sum7.print();
        sum8.print();


        // 7. 执行
        env.execute("window");
    }
}
