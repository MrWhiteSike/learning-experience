package com.bsk.flink.api.transform;

import com.bsk.flink.api.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by baisike on 2022/3/14 10:36 上午
 */
public class TransformTest2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("/Users/baisike/learning-experience/flink/bigdata-flink/src/main/resources/sensor.txt");

        // 转换成SensorReading类型
//        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String s) throws Exception {
//                String[] fields = s.split(",");
//                return new SensorReading(fields[0],Long.parseLong(fields[1]),Double.parseDouble(fields[2]));
//            }
//        });

        // lambda
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],Long.parseLong(fields[1]),Double.parseDouble(fields[2]));
        });

        // 分组
        // 可以使用 字段名 分组
//        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");

        // 还可以使用 lambda 表达式
//        KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy(data -> data.getId());
        // 还可以使用 方法引用
        KeyedStream<SensorReading, String> keyedStream2 = dataStream.keyBy(SensorReading::getId);

        // 滚动聚合，取当前最大的温度值
        SingleOutputStreamOperator<SensorReading> temperature = keyedStream2.maxBy("temperature");
        temperature.print();

        // 执行
        env.execute();
    }
}
