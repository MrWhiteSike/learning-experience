package checkpoint;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CheckpointTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 状态后端配置（3种状态后端）

        env.setStateBackend(new HashMapStateBackend());
        // 这个需要另外导入依赖
//        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///checkpoint-dir");
//        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///checkpoint-dir"));

        // 2. 检查点配置（默认是不开启的，需要使用必须开启）
        // 300ms让jobManager进行一次checkpoint检查
        env.enableCheckpointing(300);

        // 高级选项
        // checkpoint模式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // checkpoint的处理超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 最大允许同时处理几个Checkpoint（比如上一个处理到一半，这里又收到一个待处理的Checkpoint）
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // 与上面的setMaxConcurrentCheckpoints(2)冲突，这个时间间隔是当前checkpoint的处理时间与接受最新一个checkpoint之间的时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);
        // Checkpoint清理模式
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 最多能容忍几次checkpoint处理失败（默认0，即checkpoint处理失败，就当作程序执行异常）
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);

        // 3. 重启策略配置
        // 策略1：固定延迟重启策略（最多尝试3次，每次间隔10s）
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));

        // 策略2：失败率重启策略（10分钟内最多尝试3次，每次至少间隔1分钟）
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1)));

        // 策略3：指数延迟重启策略
        env.setRestartStrategy(RestartStrategies.exponentialDelayRestart(
                Time.milliseconds(1), // 指定重新启动延迟的初始值
                Time.minutes(1),  // 指定重新启动之间的最高可能持续时间
                1.1, // 每次失败后，回退值乘以这个值，直到达到 最大回退值
                Time.seconds(2),// 指定作业必须连续正确运行多长时间才能将指数增长的回退重置 为其初始值。
                0.1)); // 指定回退过程中增加或减少多大幅度的随机值。避免同时重新启动多个作业时，此选项非常有用

        /**
         *
         * 重新启动和故障转移策略:
         * 重新启动策略和故障转移策略用于控制任务重新启动。
         * 重新启动策略：决定是否以及何时可以重新启动失败或受影响的任务
         * 故障转移策略：决定应该重新启动哪些任务来恢复作业
         *
         * 重新启动策略类别	    flink-conf.yaml中的对应值	        描述
         * Fixed delay	        fixed-delay	                    固定延迟重新启动策略
         * Exponential Delay	exponential-delay	            指数延迟重新启动策略，最近新增
         * Failure rate	        failure-rate	                失败率重新启动策略
         * No restart	        none	                        直接失败，无重新启动策略
         *
         * 注意：
         * 1.如果没有启用检查点，则使用 no restart 策略
         * 2.如果检查点被激活，并且没有配置重启策略，则使用Integer.MAX_VALUE的固定延迟策略重启尝试
         * 3.通过代码设置重新启动策略时，ExecutionEnvironment同样适用。
         *
         *
         * 故障转移策略：
         * 故障转移策略类别	            jobmanager.execution.failover-strategy参数对应值	    备注
         * Restart all	                full —重启job的所有任务
         * Restart pipelined region	    region —重启job的局部的任务	                        该方式为默认值
         *
         * jobmanager.execution.failover-strategy: region
         *
         * Restart all：
         * 该策略重新启动作业中的所有任务，从任务失败中恢复
         *
         * Restart pipelined region：
         * 该策略把任务分成互不相连的区域。当检测到任务失败时，此策略计算必须重新启动以从失败中恢复的最小区域集。
            对于某些作业，与restart all相比，这会导致 重新启动的任务更少

         */

        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 9999);

        inputStream.print();
        env.execute("checkpoint test");
    }
}
