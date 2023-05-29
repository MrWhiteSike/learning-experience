package tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableTest_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(10000L);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建 输入数据表
        String inputTable = "CREATE TABLE inputTable (\n" +
                "  id STRING,\n" +
                "  times BIGINT,\n" +
                "  temperature DOUBLE" +
                ") WITH (\n" +
                "  'connector' = 'filesystem',\n" +
                "  'path' = 'file:///Users/Baisike/opensource/my-flink-examples/src/main/resources/input',\n" +
                "  'format' = 'csv',\n" +
                "  'source.monitor-interval' = '10 s'\n" +
                ")";
        tableEnv.executeSql(inputTable);

        Table table = tableEnv.sqlQuery("select id,times,temperature from inputTable");
        tableEnv.toDataStream(table, Row.class).print("sql");

        // 创建 输出到mysql的表
//        String sensor = "CREATE TABLE MySensorTable (\n" +
//                "  id STRING,\n" +
//                "  times BIGINT,\n" +
//                "  temperature DOUBLE\n" +
//                //"  PRIMARY KEY (id) NOT ENFORCED\n" +
//                ") WITH (\n" +
//                "   'connector' = 'jdbc',\n" +
//                "   'url' = 'jdbc:mysql://localhost:3306/flink_test',\n" +
//                "   'username' = 'root',\n" +
//                "   'password' = 'root',\n" +
//                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
//                "   'table-name' = 'sensor'\n" +
//                ")";
//        tableEnv.executeSql(sensor);
//        String sql = "insert into MySensorTable select id,times,temperature from inputTable";
//        tableEnv.executeSql(sql);


        // 创建 输出到file的表
        String toFileTable = "CREATE TABLE toFileTable (\n" +
                "  id STRING,\n" +
                "  times BIGINT,\n" +
                "  temperature DOUBLE" +
                ") WITH (\n" +
                "  'connector' = 'filesystem',\n" +
                "  'path' = 'file:///Users/Baisike/opensource/my-flink-examples/src/main/resources/output',\n" +
                "  'format' = 'csv',\n" +
                "  'sink.rolling-policy.rollover-interval' = '10 s'" +
                ")";
        tableEnv.executeSql(toFileTable);
        String sql = "insert into toFileTable select id,times,temperature from inputTable";
        tableEnv.executeSql(sql);
        env.execute("write file");
    }

    /**
     * 示例:
     * 展示了如何使用文件系统连接器编写流式查询语句，将数据从 Kafka 写入文件系统，然后运行批式查询语句读取数据。
     *
     * CREATE TABLE kafka_table (
     *   user_id STRING,
     *   order_amount DOUBLE,
     *   log_ts TIMESTAMP(3),
     *   WATERMARK FOR log_ts AS log_ts - INTERVAL '5' SECOND
     * ) WITH (...);
     *
     * CREATE TABLE fs_table (
     *   user_id STRING,
     *   order_amount DOUBLE,
     *   dt STRING,
     *   `hour` STRING
     * ) PARTITIONED BY (dt, `hour`) WITH (
     *   'connector'='filesystem',
     *   'path'='...',
     *   'format'='parquet',
     *   'sink.partition-commit.delay'='1 h',
     *   'sink.partition-commit.policy.kind'='success-file'
     * );
     *
     * -- 流式 sql，插入文件系统表
     * INSERT INTO fs_table
     * SELECT
     *     user_id,
     *     order_amount,
     *     DATE_FORMAT(log_ts, 'yyyy-MM-dd'),
     *     DATE_FORMAT(log_ts, 'HH')
     * FROM kafka_table;
     *
     * -- 批式 sql，使用分区修剪进行选择
     * SELECT * FROM fs_table WHERE dt='2020-05-20' and `hour`='12';
     *
     *
     *
     * 如果 watermark 被定义在 TIMESTAMP_LTZ 类型的列上并且使用 partition-time 模式进行提交，
     * sink.partition-commit.watermark-time-zone 这个属性需要设置成会话时区，否则分区提交可能会延迟若干个小时。
     * CREATE TABLE kafka_table (
     *   user_id STRING,
     *   order_amount DOUBLE,
     *   ts BIGINT, -- 以毫秒为单位的时间
     *   ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3),
     *   WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '5' SECOND -- 在 TIMESTAMP_LTZ 列上定义 watermark
     * ) WITH (...);
     *
     * CREATE TABLE fs_table (
     *   user_id STRING,
     *   order_amount DOUBLE,
     *   dt STRING,
     *   `hour` STRING
     * ) PARTITIONED BY (dt, `hour`) WITH (
     *   'connector'='filesystem',
     *   'path'='...',
     *   'format'='parquet',
     *   'partition.time-extractor.timestamp-pattern'='$dt $hour:00:00',
     *   'sink.partition-commit.delay'='1 h',
     *   'sink.partition-commit.trigger'='partition-time',
     *   'sink.partition-commit.watermark-time-zone'='Asia/Shanghai', -- 假设用户配置的时区为 'Asia/Shanghai'
     *   'sink.partition-commit.policy.kind'='success-file'
     * );
     *
     * -- 流式 sql，插入文件系统表
     * INSERT INTO fs_table
     * SELECT
     *     user_id,
     *     order_amount,
     *     DATE_FORMAT(ts_ltz, 'yyyy-MM-dd'),
     *     DATE_FORMAT(ts_ltz, 'HH')
     * FROM kafka_table;
     *
     * -- 批式 sql，使用分区修剪进行选择
     * SELECT * FROM fs_table WHERE dt='2020-05-20' and `hour`='12';
     *
     *
     *
     *
     */


}
