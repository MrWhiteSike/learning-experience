package tableapi;

import entity.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

/**
 * Apache Kafka SQL 连接器
 *
 * pom.xml中添加依赖
 * <dependency>
 *   <groupId>org.apache.flink</groupId>
 *   <artifactId>flink-connector-kafka</artifactId>
 *   <version>1.15.2</version>
 * </dependency>
 *
 *
 *
 *
 *
 */
public class TableTest_Kafka {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        // 千万注意：不要使用new！！！
        // 不然会报错：Exception in thread "main" java.lang.NullPointerException: No execution.target specified in your configuration file.
//        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 如果topic中某些分区闲置，watermark生成器将不会向前推进，
        // 可以在表配置中设置 table.exec.source.idle-timeout 选项来避免上述问题
        TableConfig config = tEnv.getConfig();
        config.set("table.exec.source.idle-timeout", "1 min");

        // 2. 创建 kafka表
        String kafkaSource = "CREATE TABLE kafkaSource (\n" +
                "  `id` STRING,\n" +
                "  `c_ts` BIGINT,\n" +
                "  `temperature` DOUBLE,\n" +
                // 以下是获取元数据在表中的定义形式，定义以及字段查询的时候 ，必须使用 `` 进行字段定义
                "  `partition` BIGINT METADATA VIRTUAL,\n" +
                "  `offset` BIGINT METADATA VIRTUAL,\n" +
                "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'sensor',\n" +
                "  'properties.bootstrap.servers' = '172.20.208.1:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")";
        tEnv.executeSql(kafkaSource);

        /**
         * 注意：
         * 1. 关键字必须使用 `` 这种执行符号进行引用。否则会报异常：org.apache.flink.table.api.SqlParserException: SQL parse failed
         *
         * 关键字：
         * 元数据 topic, partition, headers, leader-epoch, offset, timestamp, timestamp-type
         *
         *
         *
         *
         */
        String sql = "select id,c_ts,temperature,ts,`partition`,`offset` from kafkaSource where temperature>20";
        Table table = tEnv.sqlQuery(sql);
        tEnv.toDataStream(table, Row.class).print("kafka source");

        // 3. 连接kafka，写入数据
        String kafkaSink = "CREATE TABLE kafkaSink (\n" +
                "  `id` STRING,\n" +
                "  `c_ts` BIGINT,\n" +
                "  `temperature` DOUBLE,\n" +
                "  `partition` BIGINT,\n" +
                "  `offset` BIGINT,\n" +
                "  `ts` TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'sensor2',\n" +
                "  'properties.bootstrap.servers' = '172.20.208.1:9092',\n" +
                "  'format' = 'csv'\n" +
                ")";
        tEnv.executeSql(kafkaSink);

        String executeSql = "insert into kafkaSink select id,c_ts,temperature,`partition`,`offset`,ts from kafkaSource";
        tEnv.executeSql(executeSql);


        env.execute("kafka test");
    }
}
