package com.bsk.flink.hotitems;

import com.bsk.flink.beans.UserBehavior;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.*;

/**
 * Created by baisike on 2022/3/19 11:08 下午
 */
public class HotItemsWithSql {
    public static void main(String[] args) throws Exception{
        // 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings environmentSettings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);

        // 2.从kafka 消费数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer");
        // 次要参数
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<>("hotitems", new SimpleStringSchema(), properties));

        // 3.转换成POJO，提取时间戳和生成watermark
        SingleOutputStreamOperator<UserBehavior> userBehaviorDataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.of(200, TimeUnit.MILLISECONDS)) {
                    @Override
                    public long extractTimestamp(UserBehavior userBehavior) {
                        return userBehavior.getTimestamp() * 1000L;
                    }
                }
        ));

        // 4.流转换为表
        Table userTable = tableEnv.fromDataStream(userBehaviorDataStream,
                $("itemId"),
                $("behavior"),
                $("timestamp").rowtime().as("ts"));

        // 5.分组开窗聚合
        // table api
        Table windowTable = userTable
                .filter($("behavior").isEqual("pv"))
                .window(Slide.over(lit(1).hours()).every(lit(5).minutes()).on($("ts")).as("w"))
                .groupBy($("itemId"),$("w"))
                .select($("itemId"), $("w").end().as("windowEnd"), $("itemId").count().as("cnt"));

        // 6.利用开窗函数，对count值进行排序，并获取Row number，得到top N
        // sql
        DataStream<Row> aggStream = tableEnv.toAppendStream(windowTable, Row.class);
        tableEnv.createTemporaryView("agg", aggStream);
        Table resultTable = tableEnv.sqlQuery("select * from " +
                " (select *, ROW_NUMBER() over(partition by windowEnd order by cnt desc) as row_num" +
                " from agg)" +
                " where row_num <= 5 ");

        // 5.6 纯sql实现
        tableEnv.createTemporaryView("user_table", userBehaviorDataStream, $("itemId"), $("behavior"), $("timestamp").rowtime().as("ts"));
        Table resultSqlTable = tableEnv.sqlQuery("select * from " +
                "( select *,ROW_NUMBER() over(partition by windowEnd order by cnt desc) as row_num " +
                "from (" +
                "   select itemId, count(itemId) as cnt, HOP_END(ts, interval '5' minute, interval '1' hour) as windowEnd " +
                "   from user_table " +
                "   where behavior = 'pv' " +
                "   group by itemId, HOP(ts, interval '5' minute, interval '1' hour)" +
                "   )" +
                " )" +
                "where row_num <= 5 ");


        // 7.打印输出
//        tableEnv.toRetractStream(resultTable, Row.class).print();
        tableEnv.toRetractStream(resultSqlTable, Row.class).print();

        env.execute("hot items analysis with sql");
    }
}
