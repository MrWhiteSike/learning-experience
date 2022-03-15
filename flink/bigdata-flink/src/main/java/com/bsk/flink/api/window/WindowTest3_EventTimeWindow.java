package com.bsk.flink.api.window;

import com.bsk.flink.api.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * Created by baisike on 2022/3/14 11:06 下午
 */
public class WindowTest3_EventTimeWindow {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 9999);

        // 数据转换操作
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        // 使用assignTimestampsAndWatermarks引入watermark，从数据中提取时间戳，并设置延迟时间
        // 真实场景中watermark的最大乱序时间 一般是 几十/百毫秒；这里是测试数据
        SingleOutputStreamOperator<SensorReading> watermarks = dataStream
                // 升序，设置事件时间和watermark
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
//                    @Override
//                    public long extractAscendingTimestamp(SensorReading element) {
//                        return element.getTimestamp() * 1000;
//                    }
//                })
                // 乱序，设置事件时间、watermark和延迟时间
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp() * 1000;
            }
        });

        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };

        // 基于事件时间的开窗聚合，统计15秒内温度的最小值
        // watermark 是一级延迟
        // allowedLateness 是二级延迟
        // sideOutputLateData 是兜底操作，保证数据不丢失
        SingleOutputStreamOperator<SensorReading> minBy = watermarks.keyBy("id")
                .timeWindow(Time.seconds(15)) // 开窗操作
                .allowedLateness(Time.minutes(1)) // 允许再次延时1分钟的数据进行统计，来一个迟到数据计算统计一次
                .sideOutputLateData(outputTag) // 侧输出流
                .minBy("temperature");// 聚合
        minBy.print("minTemp");
        // 获取侧输出流
        minBy.getSideOutput(outputTag).print("late");

        // 后续策略：
        // 主输出流会 输出到 mysql 或者 redis 等数据存储中
        // 侧输出流如果有数据，就可以读取mysql 或者 redis中的数据，然后进行更新操作。得到正确结果
        env.execute();
    }
}
