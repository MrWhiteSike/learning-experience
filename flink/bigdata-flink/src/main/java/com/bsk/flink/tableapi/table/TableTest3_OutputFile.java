package com.bsk.flink.tableapi.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

/**
 * Created by baisike on 2022/3/17 6:43 下午
 */
public class TableTest3_OutputFile {
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

        // 3、查询转换
        // 3.1 Table API 的转换调用
        // 简单转换
        Table resultTable = inputTable.select("id, temp").filter("id === 'sensor_6'");

        // 聚合统计
        Table aggTable = inputTable.groupBy("id").select("id, id.count as count, temp.avg as avgTemp");

        // 3.2 SQL
        tableEnv.sqlQuery("select id, temp from inputTable where id = 'sensor_7'");
        Table sqlAggTable = tableEnv.sqlQuery("select id ,count(id) as cnt, avg(temp) as avgTemp from inputTable group by id");

        // 4、输出到文件
        // 连接外部文件注册输出表
        // 注意：输出到外部文件不能提前创建，就是不支持数据追加
        String outputPath = "/Users/baisike/learning-experience/flink/bigdata-flink/src/main/resources/out.txt";
        tableEnv.connect(new FileSystem().path(outputPath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
//                        .field("cnt", DataTypes.BIGINT())
//                        .field("avgTemp", DataTypes.DOUBLE())
                        .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("outputTable");

//        resultTable.insertInto("outputTable");

        // 旧版可以用下面这条
//        env.execute();

        // 新版需要用这条，用上面的执行会报错，报错如下
        // Exception in thread "main" java.lang.IllegalStateException:
        // No operators defined in streaming topology. Cannot execute.
//        tableEnv.execute("");


        // 新版的还可以用如下语句进行，执行插入操作，放到一条语句进行执行了
        resultTable.executeInsert("outputTable");

    }
}
