package com.bsk.flink.api.state;

import com.bsk.flink.api.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by baisike on 2022/3/15 6:01 下午
 */
public class StateTest2_KeyedState {
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

        dataStream.keyBy("id").map(new MyKeyCountMapper()).print();

        env.execute();
    }
    // 自定义RichMapFunction
    public static class MyKeyCountMapper extends RichMapFunction<SensorReading, Integer>{

        private ValueState<Integer> keyCountState;

        // 其他类型状态的声明
        private ListState<String> myListState;
        private MapState<String, Double> myMapState;
        private ReducingState<SensorReading> myReducingState;

        // 必须在open 中才能创建，因为getRuntimeContext() 在初始化时才可以被调用，声明类的成员变量时不能被调用
        @Override
        public void open(Configuration parameters) throws Exception {
            // 多个状态时，名称不能相同：如下的 key-count list-state
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count", Integer.class));
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<String>("list-state", String.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("map-state", String.class, Double.class));
            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>("reducing-state", new ReduceFunction<SensorReading>() {
                @Override
                public SensorReading reduce(SensorReading sensorReading, SensorReading t1) throws Exception {
                    return null;
                }
            }, SensorReading.class));
        }

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            
            // 其他状态API调用
            // List state
            Iterable<String> strings = myListState.get();
            myListState.add("");
            myListState.update(new ArrayList<>());
            
            // map state
            myMapState.get("");
            myMapState.put("1", 12.5);
            myMapState.remove("");
            myMapState.contains("");
            myMapState.isEmpty();
            myMapState.keys();
            myMapState.values();
            myMapState.entries();
            myMapState.iterator();

            // reducing state
            myReducingState.add(sensorReading);

            // 所有类型的state都有clear方法，清空当前状态的所有数据
            myReducingState.clear();


            Integer count = keyCountState.value();
            count++;
            keyCountState.update(count);
            return count;
        }
    }
}
