package state.keyed;

import entity.SensorReading;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyedStateTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(1);

        // 启用并配置Checkpoint相关参数
//        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置状态后端类型，以及存储位置
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("file:///Users/Baisike/opensource/checkpoint");

        // 本地 socket 作为数据源输入测试数据
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        // 将数据转换为SensorReading类型
        SingleOutputStreamOperator<SensorReading> dataStream = source.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // keyBy 生成 KeyedStream，自定义map方法，使用Keyed State
        SingleOutputStreamOperator<Integer> resultStream = dataStream
                .keyBy(SensorReading::getId)
                .map(new MyKeyedStateMapper());

        resultStream.print();

        env.execute("keyed state test");
    }
}
