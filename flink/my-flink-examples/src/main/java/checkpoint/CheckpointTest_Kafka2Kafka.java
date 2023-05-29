package checkpoint;

import entity.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Properties;

public class CheckpointTest_Kafka2Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointTimeout(120000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/Baisike/opensource/checkpoint");

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,Time.minutes(5)));

        String servers = "192.168.112.1:9092,192.168.112.1:9093,192.168.112.1:9094";

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(servers)
                .setTopics("test")
                .setGroupId("my_group")
//                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", "3000")
                .build();

        DataStreamSource<String> kafkaStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source1");


        SingleOutputStreamOperator<SensorReading> sensorStream = kafkaStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        SingleOutputStreamOperator<String> filterStream = sensorStream
                .filter(line -> line.getTemperature() > 30)
                .map(SensorReading::toString);

        filterStream.print("filter");
        /**
         * 以下属性在构建 KafkaSink 时是必须指定的：
         *
         * Bootstrap servers, setBootstrapServers(String)
         * 消息序列化器（Serializer）, setRecordSerializer(KafkaRecordSerializationSchema)
         * 如果使用DeliveryGuarantee.EXACTLY_ONCE 的语义保证，则需要使用 setTransactionalIdPrefix(String)
         *
         *
         * 序列化器 #
         * 构建时需要提供 KafkaRecordSerializationSchema 来将输入数据转换为 Kafka 的 ProducerRecord。
         * Flink 提供了 schema 构建器 以提供一些通用的组件，例如消息键（key）/消息体（value）序列化、topic 选择、消息分区，
         * 同样也可以通过实现对应的接口来进行更丰富的控制。
         *
         * KafkaRecordSerializationSchema.builder()
         *     .setTopicSelector((element) -> {<your-topic-selection-logic>})
         *     .setValueSerializationSchema(new SimpleStringSchema())
         *     .setKeySerializationSchema(new SimpleStringSchema())
         *     .setPartitioner(new FlinkFixedPartitioner())
         *     .build();
         * 其中消息体（value）序列化方法和 topic 的选择方法是必须指定的，此外也可以通过 setKafkaKeySerializer(Serializer)
         * 或 setKafkaValueSerializer(Serializer) 来使用 Kafka 提供而非 Flink 提供的序列化器。
         *
         *
         * 容错 #
         * KafkaSink 总共支持三种不同的语义保证（DeliveryGuarantee）。
         * 对于 DeliveryGuarantee.AT_LEAST_ONCE 和 DeliveryGuarantee.EXACTLY_ONCE，Flink checkpoint 必须启用。
         * 默认情况下 KafkaSink 使用 DeliveryGuarantee.NONE。 以下是对不同语义保证的解释：
         *
         * DeliveryGuarantee.NONE 不提供任何保证：消息有可能会因 Kafka broker 的原因发生丢失或因 Flink 的故障发生重复。
         * DeliveryGuarantee.AT_LEAST_ONCE: sink 在 checkpoint 时会等待 Kafka 缓冲区中的数据全部被 Kafka producer 确认。
         * 消息不会因 Kafka broker 端发生的事件而丢失，但可能会在 Flink 重启时重复，因为 Flink 会重新处理旧数据。
         * DeliveryGuarantee.EXACTLY_ONCE: 该模式下，Kafka sink 会将所有数据通过在 checkpoint 时提交的事务写入。
         *
         * 因此，如果 consumer 只读取已提交的数据（参见 Kafka consumer 配置 isolation.level），在 Flink 发生重启时不会发生数据重复。
         * 然而这会使数据在 checkpoint 完成时才会可见，因此请按需调整 checkpoint 的间隔。
         * 请确认事务 ID 的前缀（transactionIdPrefix）对不同的应用是唯一的，以保证不同作业的事务 不会互相影响！
         *
         * 此外，强烈建议将 Kafka 的事务超时时间调整至远大于 checkpoint 最大间隔 + 最大重启时间，
         * 否则 Kafka 对未提交事务的过期处理会导致数据丢失。
         *
         * 注意：
         * Semantic.EXACTLY_ONCE 模式依赖于事务提交的能力。事务提交发生于触发 checkpoint 之前，以及从 checkpoint 恢复之后。
         * 如果从 Flink 应用程序崩溃到完全重启的时间超过了 Kafka 的事务超时时间，那么将会有数据丢失（Kafka 会自动丢弃超出超时时间的事务）。
         * 考虑到这一点，请根据预期的宕机时间来合理地配置事务超时时间。
         *
         * 默认情况下，Kafka broker 将 transaction.max.timeout.ms 设置为 15 分钟。
         * 此属性不允许为大于其值的 producer 设置事务超时时间。
         * 默认情况下，FlinkKafkaProducer 将 producer config 中的 transaction.timeout.ms 属性设置为 1 小时，
         * 因此在使用 Semantic.EXACTLY_ONCE 模式之前应该增加 transaction.max.timeout.ms 的值，
         * 或者 修改 producer config中的 transaction.timeout.ms 属性值 < Kafka broker中transaction.max.timeout.ms设置的15分钟
         *
         * 并且 Kafka consumer 配置 isolation.level 为 read_committed （因为这个配置的默认值是read_uncommitted）
         *
         *
         * 总结：
         * 1. 开启checkpoint
         * 2. 使用DeliveryGuarantee.EXACTLY_ONCE 的语义保证 并且 使用 setTransactionalIdPrefix(String) 为事务id添加前缀
         * 3. producer config中的 transaction.timeout.ms < Kafka broker中transaction.max.timeout.ms ： 这个程序才能正常启动，否则会报异常
         * 4. Kafka consumer 配置 isolation.level 为 read_committed ，即只读已提交的数据
         * 5. producer config中的 transaction.timeout.ms （10min） >>(远远大于)  checkpoint 最大间隔（2min + 2s）+ 最大重启时间（5min）
         *
         */
        Properties kafkaProducerConfig = new Properties();
        kafkaProducerConfig.put("transaction.timeout.ms", 1000*60*10+"");
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setKafkaProducerConfig(kafkaProducerConfig)
                .setBootstrapServers(servers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("test2")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .setPartitioner(new FlinkFixedPartitioner<>())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("kafka")
                .build();

        filterStream.sinkTo(sink).name("kafka sink").setParallelism(1);


        env.execute("kafka to kafka");
    }
}
