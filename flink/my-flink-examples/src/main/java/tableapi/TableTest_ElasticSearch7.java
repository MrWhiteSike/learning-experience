package tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableTest_ElasticSearch7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        /**
         * json 格式：
         * 需要添加依赖
         *          <dependency>
         *             <groupId>org.apache.flink</groupId>
         *             <artifactId>flink-json</artifactId>
         *             <version>${flink.version}</version>
         *         </dependency>
         *
         *
         *
         *
         *
         */
        String kafkaSource = "CREATE TABLE kafkaSource (\n" +
                "  `id` STRING,\n" +
                "  `timestamp` BIGINT,\n" +
                "  `temperature` DOUBLE,\n" +
                // 以下是获取元数据在表中的定义形式，定义以及字段查询的时候 ，必须使用 `` 进行字段定义
                "  `partition` BIGINT METADATA VIRTUAL,\n" +
                "  `offset` BIGINT METADATA VIRTUAL,\n" +
                "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'test2',\n" +
                "  'properties.bootstrap.servers' = '172.24.16.1:9092',\n" +
                "  'properties.group.id' = 'testGroup2',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.ignore-parse-errors' = 'true'\n" +
                ")";
        tEnv.executeSql(kafkaSource);

        String sql = "select id,`timestamp`,temperature from kafkaSource";
        Table table = tEnv.sqlQuery(sql);
        tEnv.toDataStream(table, Row.class).print("kafka source");

        String esSink = "CREATE TABLE esSink (\n" +
                "  id STRING,\n" +
                "  `timestamp` BIGINT,\n" +
                "  temperature DOUBLE\n" +
                ") WITH (\n" +
                "  'connector' = 'elasticsearch-7',\n" +
                "  'hosts' = 'http://localhost:9200',\n" +
                "  'index' = 'sensor'\n" +
                ")";
        tEnv.executeSql(esSink);

        String insertSql = "insert into esSink select id,`timestamp`,temperature from kafkaSource";
        tEnv.executeSql(insertSql);

        env.execute("write to es");

    }
}
