package com.bsk.flink.api.transform;

import com.bsk.flink.api.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by baisike on 2022/3/14 1:43 下午
 */
public class TransformTest5_RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("/Users/baisike/learning-experience/flink/bigdata-flink/src/main/resources/sensor.txt");
        // lambda
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],Long.parseLong(fields[1]),Double.parseDouble(fields[2]));
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = dataStream.map(new MyMapper());
        map.print();


        // 执行
        env.execute();
    }

    // 自定义富函数类
    public static class MyMapper extends RichMapFunction<SensorReading, Tuple2<String, Integer>>{
        @Override
        public Tuple2<String, Integer> map(SensorReading sensor) throws Exception {
            return new Tuple2<>(sensor.getId(), getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化工作，一般是定义状态，或者建立数据库连接
            System.out.println("open");
        }

        @Override
        public void close() throws Exception {
            // 关闭连接，清理状态收尾工作
            System.out.println("close");
        }
    }
}
