package com.bsk.flink.tableapi.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
 * Created by baisike on 2022/3/17 9:31 下午
 */
public class TableTest4_KafkaPipeLine {
    public static void main(String[] args) throws Exception{
        // 创建流式环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1、创建流式表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2、连接kafka，读取数据
        tableEnv.connect(new Kafka()
                            .version("universal")
                            .topic("sensor")
                            .property("zookeeper.connect", "localhost:2181")
                            .property("bootstrap.servers", "localhost:9092"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");

        Table inputTable = tableEnv.from("inputTable");
        // 3、转换
        Table resultTable = inputTable.select("id, temp").filter("id === 'sensor_6'");
        Table aggTable = inputTable.groupBy("id").select("id, id.count as count, temp.avg as avgTemp");

        // 4、建立kafka连接，输出到不同到topic下
        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("sinktest")
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("outputTable");

        resultTable.executeInsert("outputTable");
    }
}
