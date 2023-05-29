package tableapi;

import entity.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

public class TableTest01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 读取数据
        DataStreamSource<String> inputStream = env.readTextFile("C:\\Users\\Baisike\\opensource\\my-flink-examples\\src\\main\\resources\\sensor.txt");

        // 2. 转换成pojo
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


        /**
         * 3.创建表环境：
         * 1. TableEnvironment.create
         * 2. StreamTableEnvironment.create
         */
        // 方式1：
        // planner不在使用老版本，完全采用Blink版本；废弃老版本，面向blink新版本
        // 新版本blink，真正把批处理、流处理都以DataStream实现
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // 方式2：
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 4.基于流创建一张表
        Table dataTable = tableEnv.fromDataStream(dataStream);

        // 5.调用table api进行转换操作
        Table resultTable = dataTable.select($("id"), $("temperature"))
                .where($("id").isEqual("sensor_1"));

        // 6.执行sql
        tableEnv.createTemporaryView("sensor", dataTable);
        String sql = "select id, temperature from sensor where id = 'sensor_1'";
        Table resultSqlTable = tableEnv.sqlQuery(sql);

        // 7. 打印输出
        tableEnv.toDataStream(resultTable, Row.class).print("table");
        tableEnv.toDataStream(resultSqlTable, Row.class).print("sql");

        // 8.环境执行
        env.execute("table api test");
    }
}
