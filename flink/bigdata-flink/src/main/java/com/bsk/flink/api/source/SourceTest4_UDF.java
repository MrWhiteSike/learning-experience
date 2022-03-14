package com.bsk.flink.api.source;

import com.bsk.flink.api.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * Created by baisike on 2022/3/13 10:43 下午
 */
public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<SensorReading> dataStream = env.addSource(new MySensorSource());

        // 打印输出
        dataStream.print();

        // 执行
        env.execute();
    }

    // 实现自定义Source
    public static class MySensorSource implements SourceFunction<SensorReading>{
        // 定义一个标志位，用来控制数据的产生
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
            // 定义一个随机数发生器
            Random random = new Random();
            // 设置10个传感器的初始温度
            HashMap<String, Double> sensorTemp = new HashMap<>();
            for(int i =0;i<10;i++){
                sensorTemp.put("sensor_"+(i+1), 60 + random.nextGaussian() * 20);
            }
            while (running){
                for (String sensorId:sensorTemp.keySet()) {
                    // 在当前温度基础上随机波动
                    Double newTemp = sensorTemp.get(sensorId) + random.nextGaussian();
                    sensorTemp.put(sensorId, newTemp);
                    sourceContext.collect(new SensorReading(sensorId, System.currentTimeMillis(), newTemp));
                }
                // 控制输出频率
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
