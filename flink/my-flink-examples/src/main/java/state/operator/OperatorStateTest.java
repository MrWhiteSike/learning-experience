package state.operator;

import entity.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OperatorStateTest {
    public static void main(String[] args) throws Exception {

        // 测试从checkpoint重启任务
        Configuration configuration = new Configuration();
        configuration.setString("execution.savepoint.path", "file:///Users/Baisike/opensource/checkpoint/e7ea9a6304458f925059c0649f1d4cc0/chk-10");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 每10s开始一次checkpoint
        env.enableCheckpointing(10000);

        // checkpoints 高级选项

        // 设置模式为精确一次（这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确认checkpoints之间的时间会进行5000ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        // Checkpoint 必须在1min内完成，否则就会抛异常
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 允许两个连续的checkpoint 错误
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
        // 同一时间只允许一个checkpoint进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 使用 Externalized Checkpoint,这样checkpoint 在作业取消后仍就会被保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // checkpoint 存储的配置：State Backend
        // 旧版FsStateBackend = 新版 HashMapStateBackend + FileSystemCheckpointStorage

        // 旧版
//        env.setStateBackend(new FsStateBackend("file:///Users/Baisike/opensource/checkpoint"));

        // 新版
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/Baisike/opensource/checkpoint");

        DataStream<String> inputStream = env.socketTextStream("localhost", 9999);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个有状态的map操作，统计当前分区数据个数
        SingleOutputStreamOperator<Integer> resultStream = dataStream.map(new MyCountMapper());
        resultStream.print();

//        inputStream.print();

        env.execute("state test");
    }
}
