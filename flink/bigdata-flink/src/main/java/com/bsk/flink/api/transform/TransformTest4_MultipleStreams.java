package com.bsk.flink.api.transform;

import com.bsk.flink.api.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by baisike on 2022/3/14 10:36 上午
 */
public class TransformTest4_MultipleStreams {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("/Users/baisike/learning-experience/flink/bigdata-flink/src/main/resources/sensor.txt");
        // lambda
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],Long.parseLong(fields[1]),Double.parseDouble(fields[2]));
        });

        // 单流拆分成多流的操作: 侧输出流

        // 合流connect 只能合并两条流 为ConnectedStreams<T, R>
        // ConnectedStreams.map(new CoMapFunction/CoFlatmapFunction)
        // 实现map1，map2方法/flatmap1，flatmap2方法

        // 合流 ：Union
        // 可以合并多条流，要求数据类型相同


        // 执行
        env.execute();
    }
}
