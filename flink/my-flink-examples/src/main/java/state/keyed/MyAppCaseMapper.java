package state.keyed;

import entity.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class MyAppCaseMapper extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

    // 报警的温度差 阈值
    private final Double threshold;
    // 记录上一次的温度
    private ValueState<Double> lastTemperature;

    public MyAppCaseMapper(Double threshold) {
        this.threshold = threshold;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Double> stateDescriptor = new ValueStateDescriptor<>("last-temp", Double.class);
        lastTemperature = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
        // 状态中获取上次温度
        Double lastTemp = lastTemperature.value();
        // 取出当前温度
        double currTemp = value.getTemperature();

        // 如果不为空，判断是否温差超过阈值，超过就报警
        if (lastTemp != null){
            if (Math.abs(currTemp - lastTemp) >= threshold){
                out.collect(new Tuple3<>(value.getId(), lastTemp, currTemp));
            }
        }

        // 状态更新保存为当前温度
        lastTemperature.update(currTemp);
    }

    @Override
    public void close() throws Exception {
        lastTemperature.clear();
    }
}
