package state.keyed;

import entity.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

public class MyKeyedStateMapper extends RichMapFunction<SensorReading, Integer> {

    private ValueState<Integer> valueState;

    @Override
    public Integer map(SensorReading value) throws Exception {
        // 读取状态
        Integer count = valueState.value();
        count = count == null?0:count;
        count++;
        // 更新状态
        valueState.update(count);
        return count;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 在open方法中声明一个 ValueState 类型的键控状态，因为运行时才能获取上下文信息
        ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("valuestate", Integer.class);
        valueState =  getRuntimeContext().getState(valueStateDescriptor);
    }

}
