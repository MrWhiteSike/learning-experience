package com.bsk.flink.tableapi.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * Created by baisike on 2022/3/17 4:42 下午
 */
public class TableTest2_CommonApi {

    public static void main(String[] args) throws Exception {
        // 创建流式环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1、创建流式表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2、表的创建：连接外部系统，读取数据
        // 读取文件
        String filePath = "/Users/baisike/learning-experience/flink/bigdata-flink/src/main/resources/sensor.txt";

        tableEnv.connect(new FileSystem().path(filePath)) // 定义文件系统的连接
                .withFormat(new Csv())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("timestamp", DataTypes.BIGINT())
                .field("temp", DataTypes.DOUBLE())) // 定义表结构
        .createTemporaryTable("inputTable"); // 定义临时表

        Table inputTable = tableEnv.from("inputTable");
//        inputTable.printSchema();
//        tableEnv.toAppendStream(inputTable, Row.class).print();

        // 3、查询转换
        // 3.1 Table API 的转换调用
        // 简单转换
        Table resultTable = inputTable.select("id, temp").filter("id === 'sensor_7'");

        // 聚合统计
        Table aggTable = inputTable.groupBy("id").select("id, id.count as count, temp.avg as avgTemp");

        // 3.2 SQL
        tableEnv.sqlQuery("select id, temp from inputTable where id = 'sensor_7'");
        Table sqlAggTable = tableEnv.sqlQuery("select id ,count(id) as cnt, avg(temp) as avgTemp from inputTable group by id");

        // 打印输出
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toRetractStream(aggTable, Row.class).print("agg");
        tableEnv.toRetractStream(sqlAggTable, Row.class).print("sqlAgg");

        env.execute();
    }
}
