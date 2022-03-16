package com.bsk.flink.api.state;

import com.bsk.flink.api.beans.SensorReading;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by baisike on 2022/3/15 11:07 下午
 */
public class StateTest4_StateBackend {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.状态后端设置
        env.setStateBackend(new MemoryStateBackend());
        // 需要传入 checkpoint 存放地址，（fs上的 或者 RockDb上的 一个路径）
        env.setStateBackend(new FsStateBackend(""));
        // 需要传入 checkpoint 存放地址 ，还可以通过参数设置是否可以增量进行checkpoint
        env.setStateBackend(new RocksDBStateBackend(""));


        // 2.检查点的设置
        env.enableCheckpointing(1000);// 一分钟
        // 高级选项
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // 模式
        env.getCheckpointConfig().setCheckpointTimeout(2*60*1000);// 2分钟
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100);// 为了保证处理数据
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3); //默认0，允许checkpoint失败多少次

        // 3.重启策略配置
        // 固定延迟重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
        // 失败率重启：在一定时间范围内，失败重启几次，就不能重启了；重启时间间隔
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1)));


        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 9999);

        // 数据转换操作
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        dataStream.print();

        env.execute();
    }
}
