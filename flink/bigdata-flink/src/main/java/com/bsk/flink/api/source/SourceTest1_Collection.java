package com.bsk.flink.api.source;

import com.bsk.flink.api.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Created by baisike on 2022/3/13 9:43 下午
 */
public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception{
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 按照顺序输出
        env.setParallelism(1);

        // 从集合中读取数据
        DataStream<SensorReading> dataStream = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        ));

        DataStream<Integer> integerDataStream = env.fromElements(1, 2, 4, 56, 289);

        // 打印输出
        dataStream.print("data");
        integerDataStream.print("int");

        // 执行任务
        env.execute("collection");

    }
}
