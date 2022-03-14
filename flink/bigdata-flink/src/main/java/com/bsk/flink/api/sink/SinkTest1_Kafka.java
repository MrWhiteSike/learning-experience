package com.bsk.flink.api.sink;

import com.bsk.flink.api.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * Created by baisike on 2022/3/14 2:13 下午
 */
public class SinkTest1_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        // 从文件读取数据
//        DataStream<String> inputStream = env.readTextFile("/Users/baisike/learning-experience/flink/bigdata-flink/src/main/resources/sensor.txt");


        // ETL，是英文Extract-Transform-Load的缩写，用来描述将数据从来源端经过抽取（extract）、转换（transform）、加载（load）至目的端的过程。
        // ETL一词较常用在数据仓库，但其对象并不限于数据仓库。
        // 目的是将企业中的分散、零乱、标准不统一的数据整合到一起，为企业的决策提供分析依据

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        // 下面这些是次要参数，可有可无
        properties.setProperty("group.id","consumer-group");
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset","latest");

        // 从kafka读取数据
        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), properties));

        // 数据转换操作
        DataStream<String> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],Long.parseLong(fields[1]),Double.parseDouble(fields[2])).toString();
        });

        // 输出到Kafka
        dataStream.addSink(new FlinkKafkaProducer<String>("localhost:9092","sinktest", new SimpleStringSchema()));

        env.execute();
    }
}
