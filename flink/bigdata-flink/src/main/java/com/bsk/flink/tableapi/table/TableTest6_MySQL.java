package com.bsk.flink.tableapi.table;

import com.bsk.flink.api.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Created by baisike on 2022/3/18 4:21 下午
 */
public class TableTest6_MySQL {
    public static void main(String[] args) throws Exception {
        // 创建流式环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1、创建流式表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 读取数据
        DataStreamSource<String> inputStream = env.readTextFile("/Users/baisike/learning-experience/flink/bigdata-flink/src/main/resources/sensor.txt");
        // 数据转换
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        tableEnv.createTemporaryView("sensorView", dataStream);

        // 流转表
        Table inputTable = tableEnv.fromDataStream(dataStream);

        // 聚合转换
        Table resultTable = inputTable.groupBy("id").select("id, id.count as cnt, temperature.avg as avgTemp");


        // 必须设置主键：PRIMARY KEY (id) NOT ENFORCED
        String sql = "CREATE TABLE jdbcOutputTable (\n" +
                "\tid VARCHAR(20) NOT NULL,\n" +
                "\tcnt BIGINT NOT NULL,\n" +
                "\tavgTemp DOUBLE,\n" +
                "\tPRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://localhost:3306/flink_test',\n" +
                "   'table-name' = 'sink_test',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456'\n" +
                ")";

        tableEnv.executeSql(sql);
        resultTable.executeInsert("jdbcOutputTable");

    }
}
