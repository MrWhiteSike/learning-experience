package processfunction;

import entity.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

// 需求：监控温度传感器的温度值，如果温度值在10秒钟之内（processing time）连续上升，则报警
public class MyKeyedProcessAppCase extends KeyedProcessFunction<String, SensorReading, String> {

    // 报警的时间间隔（如果在interval时间内温度持续上升，则报警）
    private Long interval;
    // 上一个温度值
    private ValueState<Double> lastTemperature;
    // 最近一次定时器的触发时间（报警时间）
    private ValueState<Long> recentTimerTimestamp;

    public MyKeyedProcessAppCase(Long interval) {
        this.interval = interval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        lastTemperature = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));
        recentTimerTimestamp = getRuntimeContext().getState(new ValueStateDescriptor<Long>("recent-timer", Long.class));
    }

    @Override
    public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
        // 当前温度
        double currTemp = value.getTemperature();
        // 上次温度（没有则设置为当前温度）
        double lastTemp = lastTemperature.value() != null ? lastTemperature.value() : currTemp;
        // 计时器状态值（时间戳）
        Long timerTimestamp = recentTimerTimestamp.value();

        // 如果 当前温度 > 上次温度 并且 没有设置报警计时器，则设置
        if (currTemp > lastTemp && timerTimestamp == null){
            long warningTimestamp = ctx.timerService().currentProcessingTime() + interval;
            ctx.timerService().registerProcessingTimeTimer(warningTimestamp);
            recentTimerTimestamp.update(warningTimestamp);
        }
        // 如果 当前温度 < 上次温度 ，并且 设置了报警计时器，则清空计时器
        else if(currTemp <= lastTemp && timerTimestamp != null){
            ctx.timerService().deleteProcessingTimeTimer(timerTimestamp);
            recentTimerTimestamp.clear();
        }
        lastTemperature.update(currTemp);
    }

    // 定时器任务
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 触发报警，并且清除定时器器状态值
        out.collect("传感器 " + ctx.getCurrentKey() + " 温度值连续" + interval + " ms上升");
        recentTimerTimestamp.clear();
    }
}
