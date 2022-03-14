package com.bsk.flink.api.transform;

import com.bsk.flink.api.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by baisike on 2022/3/14 10:36 上午
 */
public class TransformTest3_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("/Users/baisike/learning-experience/flink/bigdata-flink/src/main/resources/sensor.txt");


        // lambda
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],Long.parseLong(fields[1]),Double.parseDouble(fields[2]));
        });

        // 分组
        // 使用 方法引用 方式
        KeyedStream<SensorReading, String> keyedStream2 = dataStream.keyBy(SensorReading::getId);

        // 规约聚合 reduce：更一般常用的聚合算子
        // 定义函数
//        SingleOutputStreamOperator<SensorReading> temperature = keyedStream2.reduce(new ReduceFunction<SensorReading>() {
//            @Override
//            public SensorReading reduce(SensorReading s1, SensorReading s2) throws Exception {
//                return new SensorReading(s1.getId(), s2.getTimestamp(), Math.max(s1.getTemperature(), s2.getTemperature()));
//            }
//        });

        // lambda 表达式
        SingleOutputStreamOperator<SensorReading> temperature = keyedStream2.reduce((s1, s2) -> new SensorReading(s1.getId(), s2.getTimestamp(), Math.max(s1.getTemperature(), s2.getTemperature())));

        temperature.print();

        // 执行
        env.execute();
    }
}
