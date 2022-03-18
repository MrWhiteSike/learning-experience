package com.bsk.flink.tableapi.table;

import com.bsk.flink.api.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

/**
 * Created by baisike on 2022/3/18 9:11 下午
 */
public class TableTest7_TimeAndWindow {

    public static void main(String[] args) throws Exception {

        // 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2.读取数据
        DataStreamSource<String> inputStream = env.readTextFile("/Users/baisike/learning-experience/flink/bigdata-flink/src/main/resources/sensor.txt");

        // 定义处理时间
        // 3.转换数据
        /*SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        // 将流转换成表，定义时间特性
        Table dataTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp, pt.proctime");*/


        /**
         * 定义事件时间
         */
        // 3.转换数据
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading sensorReading) {
                return sensorReading.getTimestamp() * 1000L;
            }
        });
        // 4.将流转换成表，定义时间特性
        Table dataTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp, rt.rowtime");

//        dataTable.printSchema();
//        tableEnv.toAppendStream(dataTable, Row.class).print();

        // 执行SQL语句时，需要把table注册到内存中
        tableEnv.createTemporaryView("sensor", dataTable);


        // 5.窗口操作
        // 5.1 Group Window
        // table API
//        Table resultTable = dataTable.window(Tumble.over("10.seconds").on("rt").as("tw"))
//                .groupBy("id, tw")
//                .select("id, id.count, temp.avg, tw.end");

        // 使用Expression的 table API
        Table resultTable = dataTable.window(Tumble.over(lit(10).seconds()).on($("rt")).as("tw"))
                .groupBy($("id"), $("tw"))
                .select($("id"),$("id").count(),$("temp").avg(),$("tw").end());

        // SQL
        Table resultSqlTable = tableEnv.sqlQuery("select id, count(id) as ant, avg(temp) as avgTemp, tumble_end(rt, interval '10' second) " +
                " from sensor group by id,tumble(rt, interval '10' second)");


        // 5.2 Over Window
        // table API
//        Table overTable = dataTable.window(Over.partitionBy("id").orderBy("rt").preceding("2.rows").as("ow"))
//                .select("id,rt, id.count over ow, temp.avg over ow");

        // 使用Expression的 table API
        Table overTable = dataTable.window(Over.partitionBy("id").orderBy($("rt")).preceding(rowInterval(2L)).as("ow"))
                .select($("id"),$("rt"), $("id").count().over($("ow")),$("temp").avg().over($("ow")));

        // SQL
        Table overSqlTable = tableEnv.sqlQuery("select id, rt, count(id) over ow, avg(temp) over ow " +
                " from sensor " +
                " window ow as (partition by id order by rt rows between 2 preceding and current row)");


        dataTable.printSchema();
//        tableEnv.toAppendStream(resultTable, Row.class).print("result");
//        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");
        tableEnv.toAppendStream(overTable, Row.class).print("result");
//        tableEnv.toAppendStream(overSqlTable, Row.class).print("sql");

        env.execute();
    }
}
