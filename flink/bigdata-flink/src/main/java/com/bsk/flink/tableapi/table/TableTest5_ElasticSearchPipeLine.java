package com.bsk.flink.tableapi.table;

import com.bsk.flink.api.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Created by baisike on 2022/3/18 3:33 下午
 */
public class TableTest5_ElasticSearchPipeLine {
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

        // 流转表
        Table inputTable = tableEnv.fromDataStream(dataStream);

        // 聚合转换
        Table resultTable = inputTable.groupBy("id").select("id, id.count as count, temperature.avg as avgTemp");


        // 连接ElasticSearch，输出数据


        /*tableEnv.connect(new Elasticsearch()
        .version("7")
        .host("localhost", 9200, "http")
        .index("sensor")
        .documentType("temp"))
                .inUpsertMode()
                .withFormat(new Json())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("count", DataTypes.BIGINT())
                .field("avgTemp", DataTypes.DOUBLE()))
                .createTemporaryTable("esOutputTable");*/

        String sql = "CREATE TABLE esOutputTable (\n" +
                "  id STRING,\n" +
                "  cnt BIGINT,\n" +
                "  avgTemp DOUBLE\n" +
                ") WITH (\n" +
                "  'connector' = 'elasticsearch-7',\n" +
                "  'hosts' = 'http://localhost:9200',\n" +
                "  'index' = 'sensor'\n" +
                ")";

        tableEnv.executeSql(sql);
        resultTable.executeInsert("esOutputTable");
    }
}
