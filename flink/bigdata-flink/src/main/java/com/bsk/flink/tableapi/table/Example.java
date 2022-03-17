package com.bsk.flink.tableapi.table;

import com.bsk.flink.api.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Created by baisike on 2022/3/17 11:35 上午
 */
public class Example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 读取数据
        DataStreamSource<String> inputStream = env.readTextFile("/Users/baisike/learning-experience/flink/bigdata-flink/src/main/resources/sensor.txt");
        // 数据转换
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 创建表环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        // 基于流创建一张表
        Table table = tableEnvironment.fromDataStream(dataStream);

        //  调用table api 进行转换操作
        Table resultTable = table.select("id, temperature").where("id = 'sensor_6'");

        // 执行SQL
        tableEnvironment.createTemporaryView("sensor", table);
        String sql = "select id, temperature from sensor where id = 'sensor_6'";
        Table resultSqlTable = tableEnvironment.sqlQuery(sql);

        tableEnvironment.toAppendStream(resultTable, Row.class).print("table");
        tableEnvironment.toAppendStream(resultSqlTable, Row.class).print("sql");

        // 执行
        env.execute();
    }
}
