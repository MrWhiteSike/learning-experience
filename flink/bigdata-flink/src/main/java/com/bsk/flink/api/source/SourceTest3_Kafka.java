package com.bsk.flink.api.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * Created by baisike on 2022/3/13 10:07 下午
 */
public class SourceTest3_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        // 下面这些是次要参数，可有可无
        properties.setProperty("group.id","consumer-group");
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset","latest");

        // 从kafka读取数据
        DataStream<String> sensor = env.addSource(new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), properties));
        sensor.print();

        // 执行
        env.execute();
    }

}
