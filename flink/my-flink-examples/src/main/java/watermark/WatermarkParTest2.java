package watermark;

import entity.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 并行任务Watermark传递测试
 */
public class WatermarkParTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 修改执行环境并行度为 4 ，进行测试
        env.setParallelism(4);

        env.getConfig().setAutoWatermarkInterval(100);

        // socket文本流
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 9999);

        // 转换成SensorReading类型，分配时间戳和watermark
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        })
                // 乱序数据设置时间戳和watermark（新版）
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp() * 1000L));

        // 定义测输出流
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };

        // 基于事件事件的开窗聚合，统计15s内的温度的最小值（新版）
        SingleOutputStreamOperator<SensorReading> minTempStream = dataStream.keyBy(SensorReading::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .minBy("temperature");

        // 主流打印
        minTempStream.print("minTemp");
        // 侧输出流打印
        minTempStream.getSideOutput(outputTag).print("late");

        env.execute("watermark-par-test");

        /**
         * 启动本地socket，输入数据，查看结果:
         * nc -lk 9999
         *
         *
         * 输入：
         * sensor_1,1547718199,35.8
         * sensor_6,1547718201,15.4
         * sensor_7,1547718202,6.7
         * sensor_10,1547718205,38.1
         * sensor_1,1547718207,36.3
         * sensor_1,1547718211,34
         * sensor_1,1547718212,31.9
         * sensor_1,1547718212,31.9
         * sensor_1,1547718212,31.9
         * sensor_1,1547718212,31.9
         *
         * 注意：上面输入全部输入后，才突然有下面4条输出！
         * 输出：
         * minTemp:2> SensorReading{id='sensor_10', timestamp=1547718205, temperature=38.1}
         * minTemp:3> SensorReading{id='sensor_1', timestamp=1547718199, temperature=35.8}
         * minTemp:4> SensorReading{id='sensor_7', timestamp=1547718202, temperature=6.7}
         * minTemp:3> SensorReading{id='sensor_6', timestamp=1547718201, temperature=15.4}
         *
         * 分析：
         * 1. 计算窗口起始位置Start和结束位置End
         * 从TumblingProcessingTimeWindows类里的assignWindows方法，我们可以得知窗口的起点计算方法如下： $$ 窗口起点start = timestamp - (timestamp -offset+WindowSize) % WindowSize $$ 由于我们没有设置offset，所以这里start=第一个数据的时间戳1547718199-(1547718199-0+15)%15=1547718195
         * 计算得到窗口初始位置为Start = 1547718195，那么这个窗口理论上本应该在1547718195+15的位置关闭，也就是End=1547718210
         *
         *  // 跟踪 getWindowStartWithOffset 方法得到TimeWindow的方法
         * public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
         *   return timestamp - (timestamp - offset + windowSize) % windowSize;
         * }
         *
         * 2.计算修正后的Window输出结果的时间
         * 测试代码中Watermark设置的maxOutOfOrderness最大乱序程度是2s，所以实际获取到End+2s的时间戳数据时（达到Watermark），才认为Window需要输出计算的结果（不关闭，因为设置了允许迟到1min）
         * 所以实际应该是1547718212的数据到来时才触发Window输出计算结果。
         *
         *
         *
         * 3.为什么上面输入中，最后连续四条相同输入，才触发Window输出结果？
         *
         * 3.1 Watermark会向子任务广播
         *      我们在map才设置Watermark，map根据Rebalance轮询方式分配数据。所以前4个输入分别到4个slot中，4个slot计算得出的Watermark不同（分别是1547718199-2，1547718201-2，1547718202-2，1547718205-2）
         *
         * 3.2 Watermark传递时，会选择当前接收到的最小一个作为自己的Watermark
         *      1) 前4次输入中，有些map子任务还没有接收到数据，所以其下游的keyBy后的slot里watermark就是Long.MIN_VALUE（因为4个上游的Watermark广播最小值就是默认的Long.MIN_VALUE）
         *      2) 并行度4，在最后4个相同的输入，使得Rebalance到4个map子任务的数据的currentMaxTimestamp都是1547718212，经过getCurrentWatermark()的计算（currentMaxTimestamp-maxOutOfOrderness），4个子任务都计算得到watermark=1547718210，4个map子任务向4个keyBy子任务广播watermark=1547718210，使得keyBy子任务们获取到4个上游的Watermark最小值就是1547718210，然后4个KeyBy子任务都更新自己的Watermark为1547718210。
         *
         * 3.3 根据Watermark的定义，我们认为>=Watermark的数据都已经到达。由于此时watermark >= 窗口End，所以Window输出计算结果（4个子任务，4个结果）。
         *
         *
         * offset：窗口的偏移量
         * 时间偏移一个很大的用处是用来调准非0时区的窗口，例如：在中国你需要指定一个8小时的时间偏移，
         * 例如：
         * 1. 每日偏移8小时的滚动时间窗口
         * .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
         *
         * 2. 偏移8小时的滑动时间窗口：
         * .window(SlidingProcessingTimeWindows.of(Time.hours(12), Time.hours(1), Time.hours(-8)))
         *
         *
         *
         */
    }
}
