package checkpoint;

import entity.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.shuffle.FlinkKafkaShuffleConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class CheckpointTest_Kafka2Mysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度，为了方便测试
        env.setParallelism(1);

        // checkpoint设置
        // 每隔10s进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(10000);
        // 设置模式：exactly_once， 精确一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 检查点必须在10s之内完成，或者被丢弃【check超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(120000);
        // 确保检查点之间有2s的时间间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000);
        // 最大并发的Checkpoint数量
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 允许Checkpoint连续失败的次数【0：表示不允许失败】
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);

        // 表示一旦Flink程序被cancel后，会保留checkpoint数据，以便根据实际需要恢复到指定的checkpoint
        // env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置state backend，将检查点保存在在hdfs上面，默认保存在内存中。这里先保存到本地磁盘
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/Baisike/opensource/checkpoint");

        // 设置固定延迟重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.minutes(5)));


        /**
         * FlinkKafkaConsumer 过期了，将在 flink 1.17 被移除。可以使用 KafkaSource 代替
         */
        // kafka consumer 旧版写法如下：
        // 设置kafka消费参数
//        Properties properties = new Properties();
//        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hd1:9092,hd2:9092:hd3:9092");
//        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer-group1");
//        // kafka分区自动发现周期
//        properties.put(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "3000");

        // SimpleStringSchema可以获取kafka消息；
        // JSONKeyValueDeserializationSchema 可以获取消息的 key,value, metadata: topic, partition, offset等信息
//        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("topic_test", new SimpleStringSchema(), properties);
//        FlinkKafkaConsumer<ObjectNode> topic_test = new FlinkKafkaConsumer<>("topic_test",
//                new JSONKeyValueDeserializationSchema(true), properties);

//        DataStreamSource<String> kafkaSource = env.addSource(kafkaConsumer);


        // kafka consumer 新版写法：KafkaSource
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("10.80.224.149:9092")
                .setTopics("test")
                .setGroupId("flink_consumer_group1")
                // Kafka source 能够通过位点初始化器（OffsetsInitializer）来指定从不同的偏移量开始消费
                /**
                 * 内置的位点初始化器：
                 * 1.OffsetsInitializer.committedOffsets()  从消费者组提交的位点开始消费，不指定位点重置策略
                 * 2.OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST) 从消费组提交的位点开始消费，
                 * 如果提交位点不存在： 1）OffsetResetStrategy.EARLIEST 使用最早位点
                 *                  2）OffsetResetStrategy.LATEST 使用最新位点
                 *                  3）OffsetResetStrategy.NONE 没有位点
                 * 3.OffsetsInitializer.timestamp(1657256176000L) 从时间戳大于等于指定时间戳（毫秒）的数据开始消费
                 * 4.OffsetsInitializer.earliest() 从最早位点开始消费
                 * 5.OffsetsInitializer.latest()  从最新位点开始消费
                 *
                 * 注意：
                 * 1.如果内置的初始化器不能满足需求，也可以实现自定义的位点初始化器（OffsetsInitializer）
                 * 2.如果未指定位点初始化器，将默认使用OffsetsInitializer.earliest()，即最早位点开始消费
                 */
//                .setStartingOffsets(OffsetsInitializer.committedOffsets())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
//                .setStartingOffsets(OffsetsInitializer.timestamp(1657256176000L))
//                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setStartingOffsets(OffsetsInitializer.latest())
                // 消息解析：
                // 方式1；使用 KafkaSource 构建类中的 setValueOnlyDeserializer(DeserializationSchema) 方法，其中 DeserializationSchema 定义了如何解析 Kafka 消息体中的二进制数据
                .setValueOnlyDeserializer(new SimpleStringSchema())
                // 方式2：通过 setDeserializer(KafkaRecordDeserializationSchema) 来指定，
                // 其中 KafkaRecordDeserializationSchema 定义了如何解析 Kafka 的 ConsumerRecord，
                // 例如使用 StringDeserializer 来将 Kafka 消息体解析成字符串
//                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))


                /**
                 * 其他属性：
                 * 除了上述属性之外，还可以使用
                 * setProperties() 和 setProperty() 设置任意属性。
                 * KafkaSource有以下配置项：
                 *  client.id.prefix : 指定用于Kafka Consumer的客户端ID前缀
                 *  partition.discovery.interval.ms : 定义Kafka Source检查新分区的时间间隔。注意：分区检查功能默认 不开窍。需要显式地设置分区间隔才能启用此功能。
                 *  register.consumer.metrics : 指定是否在Flink中注册Kafka Consumer的指标
                 *  commit.offsets.on.checkpoint : 指定是否在进行checkpoint 时将消费位点提交至 Kafka broker。
                 *
                 *
                 * 消费位点提交：
                 * kafka source在checkpoint 完成时提交当前的消费位点，以保证Flink的checkpoint状态和和kafka broker 上的提交位点一致
                 * 。如果未开启checkpoint，kafka source依赖于kafka consumer 内部的位点定时自动提交逻辑，自动提交功能由
                 * enable.auto.commit 和 auto.commit.interval.ms两个kafka consumer配置项进行配置。
                 * 注意：kafka source 不依赖于 broker上提交的位点来恢复失败的作业。提交位点只是为了上报 Kafka consumer 和 消费组的消费进度
                 * ，以在broker端进行监控。
                 *
                 */
//                .setProperties()
//                .setProperty()
                .setProperty("partition.discovery.interval.ms", "3000") // 每 3 秒检查一次新分区
                .build();

        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        kafkaSource.print("kafka source");


        SingleOutputStreamOperator<SensorReading> mapStream = kafkaSource.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

//        mapStream.print("sensor");

        mapStream.addSink(new MyTwoPhaseCommitToMySql_New());

        env.execute("kafka2mysql");
    }
}
