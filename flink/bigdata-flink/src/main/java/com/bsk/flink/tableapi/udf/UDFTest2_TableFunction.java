package com.bsk.flink.tableapi.udf;

import com.bsk.flink.api.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * Created by baisike on 2022/3/19 10:41 上午
 */
public class UDFTest2_TableFunction {
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
        Table resultTable1 = dataTable
                .joinLateral(call(SplitFunction.class, $("id")))
                .select($("id"),$("ts"),$("word"),$("length"));
        // 还可以重命名拆分列
        Table resultRenameTable1 = dataTable.joinLateral(call(SplitFunction.class, $("id")).as("newWord", "newLength"))
                .select($("id"), $("ts"), $("newWord"), $("newLength"));


        // udf 使用方式2：先注册临时系统函数，然后再调用
        // 注册
        tableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);

        // table API
        Table resultTable = dataTable
                .joinLateral(call("SplitFunction",$("id")))
                .select($("id"),$("ts"),$("word"),$("length"));

        // SQL
        tableEnv.createTemporaryView("sensor", dataTable);
        // join
        Table resultSqlTable = tableEnv.sqlQuery("select id,ts,word,length from sensor,lateral table(SplitFunction(id))");
        // left join
        Table leftResultSqlTable = tableEnv.sqlQuery(
                "select id,ts,word,length " +
                "from sensor " +
                "left join lateral table(SplitFunction(id)) on true");


        // left join rename
        Table leftRenameResultSqlTable = tableEnv.sqlQuery(
                "select id,ts,newWord, newLength " +
                        "from sensor " +
                        "left join lateral table(SplitFunction(id)) AS T(newWord, newLength) on true");

//        tableEnv.toAppendStream(resultTable1, Row.class).print("result1");
//        tableEnv.toAppendStream(resultRenameTable1, Row.class).print("resultRename1");
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");
        tableEnv.toAppendStream(leftResultSqlTable, Row.class).print("left-sql");
        tableEnv.toAppendStream(leftRenameResultSqlTable, Row.class).print("left-rename-sql");

        env.execute();
    }

    @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
    public static class SplitFunction extends TableFunction<Row> {

        public void eval(String str) {
            for (String s : str.split("_")) {
                // use collect(...) to emit a row
                collect(Row.of(s, s.length()));
            }
        }
    }
}
