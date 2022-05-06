package com.bsk.flink.cdc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSQL_CDC {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 2.DDL方式建表
        tableEnv.executeSql("CREATE TABLE base_trademark (" +
                "  id INT," +
                "  tm_name STRING," +
                "  logo_url STRING" +
                ") WITH (" +
                "  'connector' = 'mysql-cdc'," +
                "  'hostname' = '192.168.36.121'," +
                "  'port' = '3306'," +
                "  'username' = 'root'," +
                "  'password' = '123456'," +
                "  'database-name' = 'gmall-flink'," +
                "  'table-name' = 'base_trademark'" +
                ")"
        );
        // 3.查询数据
        Table table = tableEnv.sqlQuery("select * from base_trademark");
        // 4.将动态表转换为流
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        retractStream.print();
        // 5.执行任务
        env.execute("FlinkSQL_CDC");
    }
}
