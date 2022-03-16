package com.bsk.flink.api.processfunction;

import com.bsk.flink.api.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Created by baisike on 2022/3/16 10:44 上午
 */
public class ProcessTest1_KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 9999);

        // 数据转换操作
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        dataStream.keyBy("id").process(new MyProcess()).print();
        env.execute();
    }
    // 实现自定义的处理函数
    public static class MyProcess extends KeyedProcessFunction<Tuple, SensorReading, Integer>{

        ValueState<Long> tsTimer;

        @Override
        public void open(Configuration parameters) throws Exception {
            tsTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context context, Collector<Integer> collector) throws Exception {
            collector.collect(value.getId().length());


            // context 可以获取的信息

            // 1、获取时间戳信息
            context.timestamp();
            // 2、获取当前Key
            context.getCurrentKey();

            // 3、设置侧输出流
//            context.output();
            // 4、注册定时器（）
            context.timerService().registerEventTimeTimer(value.getTimestamp()+1800000);
            context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + 1000);
            // 通过 状态 保存创建定时器的时间戳，用于取消这个定时器
            tsTimer.update(context.timerService().currentProcessingTime() + 1000);

            // 取消定时器：根据时间戳来区分注册的多个定时器
            context.timerService().deleteEventTimeTimer(value.getTimestamp()+1800000);
            // 获取状态中的时间戳，来取消定时器
            context.timerService().deleteProcessingTimeTimer(tsTimer.value());

            // 获取Event time定时器的当前水印
            context.timerService().currentWatermark();
            // 获取processing time定时器的当前处理时间
            context.timerService().currentProcessingTime();

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            // 定时器触发后进行的操作
            System.out.println(timestamp + "定时器触发");

            ctx.getCurrentKey();
            ctx.timerService();
//            ctx.output();
            // 时间域
            ctx.timeDomain();
            ctx.timestamp();
        }

        @Override
        public void close() throws Exception {
            // 清理工作
            tsTimer.clear();
        }
    }
}
