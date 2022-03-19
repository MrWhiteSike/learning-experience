package com.bsk.flink.tableapi.udf;

import com.bsk.flink.api.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;
import static org.apache.flink.table.api.Expressions.$;

/**
 * Created by baisike on 2022/3/19 9:20 上午
 */
public class UDFTest1_ScalarFunction {
    public static void main(String[] args) throws Exception {

        // 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2.读取数据
        DataStreamSource<String> inputStream = env.readTextFile("/Users/baisike/learning-experience/flink/bigdata-flink/src/main/resources/sensor.txt");

        // 3.转换数据
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        // 4.将流转换成表，定义时间特性
        Table dataTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp");

        // udf 使用方式1：call function "inline" without registration in Table API
        Table resultTable1 = dataTable.select($("id"),$("ts"),call(HashCode.class, $("id")));


        // udf 使用方式2：先注册临时系统函数，然后再调用
        // 注册
        tableEnv.createTemporarySystemFunction("HashCode", HashCode.class);

        // table API
        Table resultTable = dataTable.select($("id"),$("ts"),call("HashCode", $("id")));

        // SQL
        tableEnv.createTemporaryView("sensor", dataTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id,ts,HashCode(id) from sensor");


//        tableEnv.toAppendStream(resultTable1, Row.class).print("result1");
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

        env.execute();
    }

    // 自定义scalar function
    public static class HashCode extends ScalarFunction{
        public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o){
            return o.hashCode() * 10;
        }
    }


}
