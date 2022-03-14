package com.bsk.flink.api.window;

import com.bsk.flink.api.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Created by baisike on 2022/3/14 8:59 下午
 */
public class WindowTest1_TimeWindow {
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


        // 开窗测试

        // 1. 增量聚合函数：来一条计算一条
        SingleOutputStreamOperator<Integer> aggregate = dataStream.keyBy(SensorReading::getId)
//                .countWindow(10,2);
//                .window(ProcessingTimeSessionWindows.withGap(Time.minutes(1)));
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading sensorReading, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });
        // min max sum 等都是增量聚合函数


        // 2.全窗口函数: 收集起来，可以拿到更多信息，比如，窗口信息，
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> apply = dataStream.keyBy("id")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
//                .process(new ProcessWindowFunction<SensorReading, Object, Tuple, TimeWindow>() {
//                })
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<SensorReading> iterable, Collector<Tuple3<String, Long, Integer>> collector) throws Exception {
                        String id = tuple.getField(0);
                        Long windowEnd = timeWindow.getEnd();
                        int count = IteratorUtils.toList(iterable.iterator()).size();
                        collector.collect(new Tuple3<>(id, windowEnd, count));
                    }
                });

//        aggregate.print();
//        apply.print();

        // 3.其他可选API
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {};

        SingleOutputStreamOperator<SensorReading> sum = dataStream.keyBy(SensorReading::getId)
                .timeWindow(Time.seconds(15))
//                .trigger()
//                .evictor()
                // 允许延迟数据进行窗口计算: 只在 事件时间 语义下才有效
                .allowedLateness(Time.minutes(1))
                // 还有跟延迟的数据输出到侧输出流
                .sideOutputLateData(outputTag)
                .sum("temperature");

        // 获取侧数据流数据
        DataStream<SensorReading> late = sum.getSideOutput(outputTag);
        late.print();

        env.execute();
    }
}
