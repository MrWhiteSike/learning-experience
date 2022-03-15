package com.bsk.flink.api.state;

import com.bsk.flink.api.beans.SensorReading;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by baisike on 2022/3/15 11:07 下午
 */
public class StateTest4_StateBackend {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 状态后端设置
        env.setStateBackend(new MemoryStateBackend());

        // 需要传入 checkpoint 存放地址，（fs上的 或者 RockDb上的 一个路径）
        env.setStateBackend(new FsStateBackend(""));
        // 需要传入 checkpoint 存放地址 ，还可以通过参数设置是否可以增量进行checkpoint
        env.setStateBackend(new RocksDBStateBackend(""));

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 9999);

        // 数据转换操作
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        env.execute();
    }
}
