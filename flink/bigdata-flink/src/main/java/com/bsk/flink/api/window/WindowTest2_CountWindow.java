package com.bsk.flink.api.window;

import com.bsk.flink.api.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by baisike on 2022/3/14 9:59 下午
 */
public class WindowTest2_CountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
//        DataStream<String> inputStream = env.readTextFile("/Users/baisike/learning-experience/flink/bigdata-flink/src/main/resources/sensor.txt");

        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 9999);

        // 数据转换操作
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        SingleOutputStreamOperator<Double> count = dataStream.keyBy(SensorReading::getId)
                .countWindow(10, 2)
                .aggregate(new MyAvgTemp());
        count.print();

        env.execute();
    }
    public static class MyAvgTemp implements AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double>{

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        @Override
        public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> acc) {
            return new Tuple2<>(acc.f0 + value.getTemperature(), acc.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> acc) {
            return acc.f0 / acc.f1;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> acc, Tuple2<Double, Integer> acc1) {
            return new Tuple2<>(acc.f0 + acc1.f0, acc.f1 + acc1.f1);
        }
    }
}
