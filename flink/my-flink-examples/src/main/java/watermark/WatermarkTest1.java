package watermark;

import entity.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class WatermarkTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Flink1.12.X 已经默认就是使用EventTime了，所以不需要这行代码
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 默认周期是200毫秒，可以使用 ExecutionConfig.setAutoWatermarkInterval() 方法进行设置，例如：
        env.getConfig().setAutoWatermarkInterval(100);

        // socket 文本流作为数据源
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 9999);


        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 升序数据设置事件时间和watermark（旧版）
        SingleOutputStreamOperator<SensorReading> ascStream = dataStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
            @Override
            public long extractAscendingTimestamp(SensorReading element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // 升序数据设置事件时间和watermark（新版）
        SingleOutputStreamOperator<SensorReading> ascStream1 = dataStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<SensorReading>forMonotonousTimestamps()
                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp() * 1000L));


        // 乱序数据设置时间戳和watermark (旧版)
        SingleOutputStreamOperator<SensorReading> boundOutStream = dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // 乱序数据设置时间戳和watermark（新版）
        SingleOutputStreamOperator<SensorReading> boundOutStream1 = dataStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(((element, recordTimestamp) -> element.getTimestamp() * 1000L)));

        /**
         * WatermarkStrategy 简介：
         *
         * 使用 Flink API 时需要设置一个同时包含 TimestampAssigner (事件时间提取器) 和 WatermarkGenerator （watermark生成器） 的 WatermarkStrategy。
         * WatermarkStrategy 工具类中也提供了许多常用的 watermark 策略，并且用户也可以在某些必要场景下构建自己的 watermark 策略。
         *
         * 通常情况下，你不用实现此接口，而是可以使用 WatermarkStrategy 工具类中通用的 watermark 策略，
         * 或者可以使用这个工具类将自定义的 TimestampAssigner 与 WatermarkGenerator 进行绑定。
         *
         * 其中 TimestampAssigner 的设置与否是可选的，大多数情况下，可以不用去特别指定。
         * 例如，当使用 Kafka 或 Kinesis 数据源时，你可以直接从 Kafka/Kinesis 数据源记录中获取到时间戳。
         *
         * 注意： 时间戳和 watermark 都是从 1970-01-01T00:00:00Z 起的 Java 纪元开始，并以毫秒为单位。
         */

        /**
         * 使用 Watermark 策略:
         * WatermarkStrategy 可以在 Flink 应用程序中的两处使用，第一种是直接在数据源上使用，第二种是直接在非数据源的操作之后使用。
         * 第一种方式相比会更好，因为数据源可以利用 watermark 生成逻辑中有关分片/分区（shards/partitions/splits）的信息。
         * 使用这种方式，数据源通常可以更精准地跟踪 watermark，整体 watermark 生成将更精确。直接在源上指定 WatermarkStrategy 意味着你必须使用特定数据源接口
         * 参阅 https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/dev/datastream/event-time/generating_watermarks/#watermark-%E7%AD%96%E7%95%A5%E4%B8%8E-kafka-%E8%BF%9E%E6%8E%A5%E5%99%A8
         * 了解如何使用Kafka Connector， 以及有关每个分区的watermark是如何生成以及工作的。
         *
         * 第二种方式：在任意转换操作之后设置 WatermarkStrategy
         * 仅当无法直接在数据源上设置策略时，才应该使用第二种方式（在任意转换操作之后设置 WatermarkStrategy）。
         *
         * 使用 WatermarkStrategy 去获取流并生成带有时间戳的元素和 watermark 的新流时，
         * 如果原始流已经具有时间戳或 watermark，则新指定的时间戳分配器将覆盖原有的时间戳和 watermark。
         *
         */


        /**
         * 处理空闲数据源:
         *
         * 如果数据源中的某一个分区/分片在一段时间内未发送事件数据，则意味着 WatermarkGenerator 也不会获得任何新数据去生成 watermark。我们称这类数据源为空闲输入或空闲源。在这种情况下，当某些其他分区仍然发送事件数据的时候就会出现问题。由于下游算子 watermark 的计算方式是取所有不同的上游并行数据源 watermark 的最小值，则其 watermark 将不会发生变化。
         *
         * 为了解决这个问题，你可以使用 WatermarkStrategy 来检测空闲输入并将其标记为空闲状态。WatermarkStrategy 为此提供了一个工具接口：
         *  WatermarkStrategy
         *         .<Tuple2<Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(20))
         *         .withIdleness(Duration.ofMinutes(1));
         */


        /**
         * 自定义 WatermarkGenerator:
         * watermark 的生成方式本质上是有两种：周期性生成和标记生成。
         * 周期性生成器通常通过 onEvent() 观察传入的事件数据，然后在框架调用 onPeriodicEmit() 时发出 watermark。
         * 标记生成器将查看 onEvent() 中的事件数据，并等待检查在流中携带 watermark 的特殊标记事件或打点数据。当获取到这些事件数据时，它将立即发出 watermark。通常情况下，标记生成器不会通过 onPeriodicEmit() 发出 watermark。
         *
         * 1.自定义周期性 Watermark 生成器
         * 周期性生成器会观察流事件数据并定期生成 watermark（其生成可能取决于流数据，或者完全基于处理时间）。
         * 生成 watermark 的时间间隔（每 n 毫秒）可以通过 ExecutionConfig.setAutoWatermarkInterval(...) 指定。每次都会调用生成器的 onPeriodicEmit() 方法，如果返回的 watermark 非空且值大于前一个 watermark，则将发出新的 watermark。
         * 如下是两个使用周期性 watermark 生成器的简单示例。注意：Flink 已经附带了 BoundedOutOfOrdernessWatermarks，它实现了 WatermarkGenerator，其工作原理与下面的 BoundedOutOfOrdernessGenerator 相似。
         *
         * 2.自定义标记 Watermark 生成器
         * 标记 watermark 生成器观察流事件数据并在获取到带有 watermark 信息的特殊事件元素时发出 watermark。
         * 如下是实现标记生成器的方法，当事件带有某个指定标记时，该生成器就会发出 watermark：
         * public class PunctuatedAssigner implements WatermarkGenerator<MyEvent> {
         *
         *     @Override
         *     public void onEvent(MyEvent event, long eventTimestamp, WatermarkOutput output) {
         *         if (event.hasWatermarkMarker()) {
         *             output.emitWatermark(new Watermark(event.getWatermarkTimestamp()));
         *         }
         *     }
         *
         *     @Override
         *     public void onPeriodicEmit(WatermarkOutput output) {
         *         // onEvent 中已经实现
         *     }
         * }
         *
         * 注意： 可以针对每个事件去生成 watermark。但是由于每个 watermark 都会在下游做一些计算，因此过多的 watermark 会降低程序性能。
         *
         */

        /**
         * Watermark策略与kafka连接器
         * 当使用 Apache Kafka 连接器作为数据源时，每个Kafka分区可能有一个简单的事件时间模式（递增的时间戳或有界无序）
         * 然而，当使用kafka数据源时，多个分区常常并行使用，因此交错来自各个分区的事件数据就会破坏每个分区的事件时间模式（这是Kafka消费客户端所固有的）
         *
         * 在这种情况下，你可以使用Flink中可识别Kafka分区的watermark生成机制。使用此特性，将在Kafka消费端内部针对每个Kafka分区生成watermark
         * ，并且不同分区watermark的合并方式与在数据流shuffle时的合并方式相同。
         *
         * 例如，如果每个Kafka分区中的事件时间戳严格递增，则使用 单调递增时间戳分配器 按分区生成的watermark，将生成完美的全局watermark。
         * 注意，我们在示例中未使用TimestampAssigner，而是使用了Kafka记录自身的时间戳。
         *
         * KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
         *     .setBootstrapServers(brokers)
         *     .setTopics("my-topic")
         *     .setGroupId("my-group")
         *     .setStartingOffsets(OffsetsInitializer.earliest())
         *     .setValueOnlyDeserializer(new SimpleStringSchema())
         *     .build()
         *
         * DataStream<String> stream = env.fromSource(
         *     kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)), "mySource");
         *
         */

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("")
                .setTopics("my-topic")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> mySource = env.fromSource(
                kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)),"mySource"
        );

        /**
         * 算子处理watermark的方式：
         * 一般情况下，在将watermark转发到下游之前，需要算子对其进行触发的事件完全进行处理。
         * 例如，WindowOperator将首先计算该watermark触发的所有窗口数据，当且仅当由此watermark触发计算进而生成的所有数据被转发到下游之后，其才会被发送到下游。
         *
         * 相同的规则也适用于TwoInputStreamOperator。但是，在这种情况下，算子当前的watermark会取其两个输入的最小值。
         *
         */

        // 定义测输出流
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };

        // 基于事件事件的开窗聚合，统计15s内的温度的最小值（旧版）
        SingleOutputStreamOperator<SensorReading> minTempStream = boundOutStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .minBy("temperature");

        // 打印输出
        minTempStream.print("minTemp");
        minTempStream.getSideOutput(outputTag).print("late");


        env.execute("watermark-test");
    }
}
