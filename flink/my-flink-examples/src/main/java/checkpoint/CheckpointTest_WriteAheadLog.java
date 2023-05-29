package checkpoint;

import entity.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 事务型接收器：
 * 事务型连接器需要与Flink的检查点机制集成，因为它们只能在检查点成功完成时将数据提交到外部系统。
 *
 * 为了简化事务性接收器的实现，Flink的DataStream API 提供了两个模板，可以扩展它们来实现自定义接收算子。
 * 两个模板都实现了CheckpointListener接口来接收来自JobManager关于完成的检查点的通知。
 *
 * GenericWriteAheadSink模板：收集每个检查点的所有输出记录，并将它们存储在sink任务的算子状态。在失败的情况下，状态被检查点恢复。
 * 当任务收到检查点完成通知时，将完成的检查点的记录写入外部系统。带有WAL-enabled的Cassandra Sink连接器实现了这个接口。
 *
 * TwoPhaseCommitSinkFunction模板：利用了外部接收器系统的事务特性，对于每个检查点，它启动一个新事务，并在当前事务的上下文中将所有记录写入接收器。
 * 接收器在接收到相应检查点的完成通知时提交事务。
 *
 *
 * 扩展GenericWriteAheadSink的算子需要提供三个构造函数参数:
 *  一个CheckpointCommitter，见前面章节介绍。
 *  一个TypeSerializer用于序列化输入记录。
 *  传递给CheckpointCommitter的作业ID，以标识跨应用程序重新启动的提交信息
 *
 *  write-ahead运算符需要实现一个单一的方法:
 *  boolean sendValues(Iterable<IN> values, long checkpointId, long timestamp)
 *  调用sendValues()方法将完成的检查点的记录写入外部存储系统。
 *  该方法接收一个Iterable(包括检查点的所有记录)、一个检查点的ID和一个生成检查点的时间戳。
 *  如果所有写操作都成功，则该方法必须返回true；如果写操作失败，则返回false。
 *
 *  GenericWriteAheadSink不能提供精确一次保证，只能提供至少一次的保证。有两种故障情况会导致记录被输出不止一次:
 *  1. 当任务运行sendValues()方法时，程序失败。如果外部sink系统不能原子地写入多个记录（要么全部写入，要么全部失败），而是部分记录可能被写入。
 *  由于检查点尚未提交，所以在恢复期间sink系统将被再次写入所有记录。
 *
 *  2. 所有记录都正确写入，sendValues()方法返回true；但是，在调用CheckpointCommitter或CheckpointCommitter未能提交检查点之前，程序失败。
 *  在恢复期间，所有尚未提交的检查点记录将被重新写入。
 *  原因：检查点分两个步骤提交。首先，接收器持续存储已提交的检查点信息，然后从WAL中删除记录。将提交信息存储子啊Flink的应用程序状态中，它不是
 *  持久性的，并且在出现故障时将被重置。相反，GenericWriteAheadSink依赖于一个名为CheckpointCommitter的可插入组件来存储和查找关于外部持久
 *  存储中提交的检查点信息。
 *
 *
 * TwoPhaseCommitSinkFunction：
 * 问题：2PC协议是不是代价太大了?
 * 通常，两阶段提交（2PC）是确保分布式系统一致性的方法，代价相对来说比较大。
 * 但是，在Flink上下文中，协议对于每个检查点只运行一次。此外，TwoPhaseCommitSinkFunction协议利用了Flink的常规检查点机制，因此增加的开销很小。
 * TwoPhaseCommitSinkFunction的工作原理与WAL Sink非常相似，但是它不会收集Flink应用状态下的记录；相反，它将它们以开放事务的形式写入外部系统。
 *
 * TwoPhaseCommitSinkFunction实现协议：在sink任务发出第一个记录之前，它在外部系统上启动一个事务。所有随后收到的记录都是在事务的上下文
 * 中写入的。当JobManager启动一个检查点并在启动程序的源中注入barriers时，2PC协议的投票阶段就开始了。当算子接收到barriers时，它会保持状态，
 * 并在完成之后向JobManager发送确认消息。当sink任务接收到barriers时，它将持久化该状态，准备提交当前事务，并在JobManager上确认检查点。
 * JobManager的确认消息类似于2PC协议的提交投票。Sink任务还不能提交事务，因为不能保证作业的所有任务都将完成其检查点。Sink任务还为下一个检查点
 * barrier之前到达的所有记录启动一个事务。
 *
 * 当JobManager从所有任务实例接收到成功的检查点通知时，它会向所有的任务发送检查点完成的通知，此通知对应于2PC协议的提交命令。
 * 当接收器任务接收到通知时，它将提交以前检查点的所有打开的事务。sink任务一旦确认其检查点，就必须能够提交相应的事务，即使在出现故障的情况下也是如此。
 * 如果无法提交事务，则接收器将丢失数据。当所有sink任务提交它们的事务时，2PC协议的迭代就算成功了。
 *
 * 外部系统的要求：
 * 1.外部sink系统必须提供事务，或sink必须能够模拟外部系统上的事务。 sink能够向sink系统写入数据，但是写入的数据在提交之前不能对外公开。
 * 2.在检查点期间，事务必须开启并接受写操作
 * 3.事务必须等到接收到检查点完成通知时，再提交。
 * 4.处理一旦失败，sink必须能够恢复事务。一些sink系统提供一个事务ID可用于提交或中止一个开启的事务。
 * 5.提交一个事务必须是一个幂等操作，sink或外部系统应该能够做到：一个事务已经提交或重复提交，没有影响。
 *
 */
public class CheckpointTest_WriteAheadLog {
    public static void main(String[] args) throws Exception {

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 1. 开启checkpoint并配置
        env.enableCheckpointing(10000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(120000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000L);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 2. 状态后端配置
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/Baisike/opensource/checkpoint");

        // 3. 重启策略配置
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.minutes(5)));


        // 4. 读取socket流
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 9999);

        // 5. 转换为pojo
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 6. 过滤温度>30的数据
        SingleOutputStreamOperator<SensorReading> filter = dataStream.filter(new FilterFunction<SensorReading>() {
            @Override
            public boolean filter(SensorReading value) throws Exception {
                return value.getTemperature() > 30;
            }
        });

        filter.print("filter");

        // 7. 采用 预写日志方式（Write Ahead Log）输出数据到mysql
        MySinkToMySql mySinkToMySql = new MySinkToMySql();
        filter.transform("mysql", TypeInformation.of(SensorReading.class), mySinkToMySql);

        // 8. 执行
        env.execute("write ahead log");
    }
}
