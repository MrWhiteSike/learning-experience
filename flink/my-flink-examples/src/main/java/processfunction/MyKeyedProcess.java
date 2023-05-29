package processfunction;

import entity.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class MyKeyedProcess extends KeyedProcessFunction<String, SensorReading, Integer> {
    ValueState<Long> tsTimerState;
//    OutputTag<Integer> outputTag;
//
//    public MyKeyedProcess(OutputTag<Integer> outputTag) {
//        this.outputTag = outputTag;
//    }

    @Override
    public void open(Configuration parameters) throws Exception {
        tsTimerState = getRuntimeContext().getState(new ValueStateDescriptor<>("ts-timer-state", Long.class));
    }

    @Override
    public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws Exception {
        out.collect(value.getId().length());

        // 获取当前处理元素的时间戳 或者 一个定时器的时间戳
//        ctx.timestamp();
//
//        // 获取当前处理元素的key
//        ctx.getCurrentKey();
//
//        // 通过timerService获取当前的处理时间和当前的水位线
//        ctx.timerService().currentProcessingTime();
//        ctx.timerService().currentWatermark();
//
//        // 在处理时间的5秒延迟后触发
        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000L);
        tsTimerState.update(ctx.timerService().currentProcessingTime() + 5000L);
//
//        // 在事件时间的10秒延迟后触发
//        ctx.timerService().registerEventTimeTimer((value.getTimestamp() + 10) * 1000L);
//        tsTimerState.update((value.getTimestamp() + 10) * 1000L);
//
//        // 删除指定时间触发的定时器
//        ctx.timerService().deleteProcessingTimeTimer(tsTimerState.value());
//        ctx.timerService().deleteEventTimeTimer(tsTimerState.value());
//
//        // side output 测输出流
//        if (value.getId().length() > 10){
//            ctx.output(outputTag, value.getId().length());
//        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
        System.out.println(timestamp + " 定时器触发");

        // 获取timerService
        // ctx.timerService();


        // 获取key
//        ctx.getCurrentKey();
//        // 获取时间戳
//        ctx.timestamp();
//        // 获取时间域
//        ctx.timeDomain();
//        // 输出测输出流
//        ctx.output(outputTag, ctx.getCurrentKey().length());
    }

    @Override
    public void close() throws Exception {
        tsTimerState.clear();
    }
}
