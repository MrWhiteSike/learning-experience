package tableapi;

import entity.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class TableTest_TimeAndWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 读入文件数据，得到DataStream
        DataStream<String> inputStream = env.readTextFile("C:\\Users\\Baisike\\opensource\\my-flink-examples\\src\\main\\resources\\sensor.txt");

        // 转换为POJO
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


        // 定义 处理时间 （Processing Time）
        // 方式1：DataStream到Table转换时定义
//        Table dataTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp, pt.proctime");
//        Table dataTable = tableEnv.fromDataStream(dataStream, $("id"), $("timestamp"), $("temperature"), $("pt").proctime());

        // 方式2：创建表的DDL中定义
//        String sql = "CREATE TABLE sensor (\n" +
//                "  id STRING,\n" +
//                "  ts BIGINT,\n" +
//                "  temperature DOUBLE,\n" +
//                "   pt as PROCTIME()\n" +
//                ") WITH (\n" +
//                "  'connector' = 'filesystem',\n" +
//                "  'path' = 'file:///Users/Baisike/opensource/my-flink-examples/src/main/resources/input/sensor.txt',\n" +
//                "  'format' = 'csv'\n" +
////                "  'source.monitor-interval' = '10 s'\n" +
//                ")";
//        tableEnv.executeSql(sql);
//        Table dataTable = tableEnv.sqlQuery("select * from sensor");

        // Schema 无法添加处理时间？为啥不能添加呢
//        Table t = tableEnv.fromDataStream(dataStream, Schema
//                .newBuilder()
//                .column("id", DataTypes.STRING())
//                .column("timestamp", DataTypes.BIGINT())
//                .column("temperature", DataTypes.DOUBLE())
////                .columnByExpression("pt", $("pt").proctime().toExpr())
//                .build()
//        );
//        Table dataTable = t.addColumns($("pt").proctime());


        // 定义 事件时间 （Event Time）

        SingleOutputStreamOperator<SensorReading> timestampsAndWatermarks = dataStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(((element, recordTimestamp) -> element.getTimestamp() * 1000L)));


        // 指定已有字段为事件时间字段
//        Table dataTable = tableEnv.fromDataStream(timestampsAndWatermarks, "id, timestamp.rowtime as ts, temperature as temp");
        Table dataTable = tableEnv.fromDataStream(timestampsAndWatermarks, $("id"), $("timestamp").rowtime(), $("temperature"));
        // 直接追加时间字段
//        Table dataTable = tableEnv.fromDataStream(timestampsAndWatermarks, "id, timestamp as ts, temperature as temp, rt.rowtime");
//        Table dataTable = tableEnv.fromDataStream(timestampsAndWatermarks, "id, timestamp as ts, temperature as temp, rt.rowtime");
//        Table dataTable = tableEnv.fromDataStream(timestampsAndWatermarks, $("id"), $("timestamp"), $("temperature"), $("rt").rowtime());


        dataTable.printSchema();
        tableEnv.toDataStream(dataTable, Row.class).print();

        env.execute();

    }
}
