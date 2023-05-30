package tableapi;

import entity.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

public class TableTest2_TimeAndWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 配置时区
        TableConfig config = tableEnv.getConfig();
//        config.set("table.local-time-zone", "UTC");
        config.set("table.local-time-zone", "Asia/Shanghai");

        // 读入文件数据，得到DataStream
        DataStream<String> inputStream = env.readTextFile("C:\\Users\\Baisike\\opensource\\my-flink-examples\\src\\main\\resources\\sensor.txt");

        // 转换为POJO
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(((element, recordTimestamp) -> element.getTimestamp() * 1000L)))
                ;
        // 定义时间属性
        Table dataTable = tableEnv.fromDataStream(dataStream, $("id"), $("timestamp").rowtime().as("ts"), $("temperature"));

//                String sql = "CREATE TABLE sensor (\n" +
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
//
//        Table dataTable = tableEnv.sqlQuery("select * from sensor");


        dataTable.printSchema();

        tableEnv.toDataStream(dataTable, Row.class).print();

        // 1. Table API
        // 1.1 Group Window 聚合操作
        Table select = dataTable.window(Tumble.over(lit(10).seconds()).on($("ts")).as("w"))
//        Table select = dataTable.window(Tumble.over(rowInterval((long) 2)).on($("pt")).as("w"))
                .groupBy($("id"),$("w"))
                .select($("id"),$("id").count(),$("temperature").avg(),$("w").start(),$("w").end(),$("w").rowtime());
        // 1.2 Over Window 聚合操作
        Table select1 = dataTable.window(Over
                .partitionBy($("id"))
                .orderBy($("ts"))
                .preceding(UNBOUNDED_RANGE)
                .as("ow"))
                .select($("id"), $("temperature").max().over($("ow")));

//        select.printSchema();

        // 2. SQL
        // 2.1 Group Window （过时了，但也可以用）
        /**
         * Group Window Functions
         * 1. TUMBLE(time_attr, interval):  tumbling time window
         * 2. HOP(time_attr, interval, interval): hopping time window(sliding window)
         * 3. SESSION(time_attr, interval)：session time window
         *
         * 注意：
         * 1. TUMBLE,HOP和SESSION可以在事件时间（流+批）或处理时间（流）上定义窗口
         * 2. 在流模式下，组窗口函数的time_attr参数必须引用一个有效的时间属性，该属性指定行的处理时间或事件时间。
         * 在批处理模式下，组窗口函数的time_attr参数必须是TIMESTAMP类型的属性。
         *
         * 组窗口的开始和结束时间戳以及时间属性可以通过以下辅助函数进行选择：
         * 开始时间：
         * TUMBLE_START(time_attr, interval)
         * HOP_START(time_attr, interval, interval)
         * SESSION_START(time_attr, interval)
         *
         * 返回相应滚动、滑动或会话窗口的包含下限的时间戳。
         *
         * 结束时间：
         * TUMBLE_END(time_attr, interval)
         * HOP_END(time_attr, interval, interval)
         * SESSION_END(time_attr, interval)
         *
         * 返回相应滚动、滑动或会话窗口的不包含上限的时间戳。
         * 注意：在随后的基于时间的操作（比如 间隔联接（ interval joins ） 和组窗口聚合（group window）或开窗聚合（over window））中，
         * 上限时间戳不能用作行时间属性。
         *
         * 事件时间：
         * TUMBLE_ROWTIME(time_attr, interval)
         * HOP_ROWTIME(time_attr, interval, interval)
         * SESSION_ROWTIME(time_attr, interval)
         *
         * 生成的属性是事件时间属性，可用于后续基于时间的操作，比如 间隔联接（ interval joins ） 和组窗口聚合（group window）或开窗聚合（over window）。
         *
         * 处理时间：
         * TUMBLE_PROCTIME(time_attr, interval)
         * HOP_PROCTIME(time_attr, interval, interval)
         * SESSION_PROCTIME(time_attr, interval)
         *
         * 返回的处理事件属性值，可用于后续基于时间的操作，比如 间隔联接（ interval joins ） 和组窗口聚合（group window）或开窗聚合（over window）。
         *
         * 注意：必须使用与group BY子句中的组窗口函数完全相同的参数来调用辅助函数。
         *
         * 例子：
         * CREATE TABLE Orders (
         *   user       BIGINT,
         *   product    STRING,
         *   amount     INT,
         *   order_time TIMESTAMP(3),
         *   WATERMARK FOR order_time AS order_time - INTERVAL '1' MINUTE
         * ) WITH (...);
         *
         * SELECT
         *   user,
         *   TUMBLE_START(order_time, INTERVAL '1' DAY) AS wStart,
         *   SUM(amount) FROM Orders
         * GROUP BY
         *   TUMBLE(order_time, INTERVAL '1' DAY),
         *   user
         *
         */

        tableEnv.createTemporaryView("sensor", dataTable);
        Table sql = tableEnv.sqlQuery("select id,count(id) as cnt, avg(temperature) as avgTemp," +
                "tumble_start(ts, interval '10' second) as startTime, " +
                "tumble_end(ts, interval '10' second) as endTime " +
                " from sensor group by id, tumble(ts, interval '10' second)");

        // 2.2 Over Window
        /**
         * 有两种选项进行定义范围：
         * 1. RANGE intervals
         *  RANGE间隔是在ORDER BY列的值上定义的，在Flink的情况下，这始终是一个时间属性。以下RANGE间隔定义了时间属性最多比当前行少30分钟的所有行都包含在聚合中：
         *
         *  RANGE BETWEEN INTERVAL '30' MINUTE PRECEDING AND CURRENT ROW
         *
         *
         * 2. ROWS intervals
         * ROWS间隔是基于计数的间隔。它精确地定义了聚合中包含的行数。以下ROWS间隔定义了当前行和当前行之前的10行（因此总共11行）包含在集合中：
         *
         * ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
         *
         */
        Table sql1 = tableEnv.sqlQuery("select id, max(temperature) over ow as maxTemp" +
                " from sensor " +
                " window ow as (partition by id order by ts rows between 2 preceding and current row)");

        tableEnv.toDataStream(select, Row.class).print("table group>>>>>");
        tableEnv.toDataStream(select1, Row.class).print("table over>>>>>");

        tableEnv.toDataStream(sql, Row.class).print("sql group>>>>>");
        tableEnv.toDataStream(sql1, Row.class).print("sql over>>>>>");

        // 3. Window TVF(table-valued functions，表值函数) Aggregation: Tumble Windows Hop Windows Cumulate Windows Session Windows (will be supported soon)
        // Group Window Aggregation 过时了，我们鼓励使用更强大、更有效的Window TVF聚合。
        /**
         * Window TVF是Flink定义的多态表函数(Polymorphic Table Functions, 缩写PTF), PTF是SQL 2016标准的一部分，是一个特殊的表函数，但可以将表作为参数.
         * PTF是更改表格形状的强大功能。因为PDF在语义上与表类似，所以它们的调用发生在SELECT语句的FROM子句中。
         * Window TVF是对传统分组窗口函数的替代.
         * Window TVF更符合SQL标准，更强大地支持复杂的基于窗口的计算. 比如, Window TopN, Window Join
         * 但是，分组窗口函数只能支持窗口聚合。
         *
         * Flink支持 TUMBLE、HOP和CUMULATE类型的窗口聚合。在流模式中，Window TVF（窗口表值函数）的时间属性字段必须基于事件或处理时间属性。
         * 有关更多窗口功能信息，请参阅窗口TVF。在批处理模式下，窗口表值函数的时间属性字段必须是TIMESTAMP或TIMESTAMP_LTZ类型的属性
         *
         *
         * 查看更多如何应用基于窗口TVF的进一步计算：
         * Window Aggregation ：窗口聚合
         * Window TopN ：窗口 TopN
         * Window Join ：窗口 Join
         * Window Deduplication ：窗口去重
         * 限制：
         * 1. 窗口去重紧跟的窗口TVF的限制：目前，如果紧跟着窗口TVF之后执行窗口去重，则窗口TVF必须使用TUMBLE窗口、HOP窗口或CUMULATE窗口，
         * 而不是Session 窗口。Session 窗口将在不久的将来得到支持。
         * 2. 排序字段的时间属性限制：当前，窗口重复数据删除要求顺序键必须是事件时间属性，而不是处理时间属性。不久的将来将支持按处理时间排序。
         *
         *
         *
         * Apache Flink提供了3个内置窗口TVF:TUMBLE、HOP和CUMULATE。开窗TVF的返回值是一个新的关系，
         * 它包括原始关系的所有列，以及另外3列，名为“window_start”、“window_end”和“window_time”，以指示分配的窗口。
         * 在流模式中，“window_time”字段是窗口的时间属性。
         * 在批处理模式中，“window_time”字段是基于输入时间字段类型的TIMESTAMP或TIMESTAMP_LTZ类型的属性。
         * “window_time”字段可用于后续基于时间的操作，例如另一个窗口TVF或聚合上的间隔联接。
         * window_time的值始终等于window_end-1ms。
         *
         * TUMBLE函数需要三个必需参数，一个可选参数：
         * TUMBLE(TABLE data, DESCRIPTOR(timecol), size [, offset ])
         *  data：是一个表参数，可以是与时间属性列的任何关系。
         *  timecol：是一个列描述符，指示数据的哪个时间属性列应映射到翻滚窗口。
         *  size：指定翻滚窗口宽度的持续时间。
         *  offset：是一个可选参数，用于指定窗口起点将偏移的偏移量。
         *
         * HOP采用四个必需参数，一个可选参数：
         * HOP(TABLE data, DESCRIPTOR(timecol), slide, size [, offset ])
            data：是一个表参数，可以是与时间属性列的任何关系。
         *  timecol：是一个列描述符，指示数据的哪个时间属性列应映射到翻滚窗口。
         *  slide：是一个持续时间，指定连续跳变窗口开始之间的持续时间，即窗口滑动时间
         *  size：指定HOP窗口宽度的持续时间。
         *  offset：是一个可选参数，用于指定窗口起点将偏移的偏移量。
         *
         *
         * CUMULATE采用四个必需参数，一个可选参数：
         * CUMULATE(TABLE data, DESCRIPTOR(timecol), step, size [, offset ])
         *  data：是一个表参数，可以是与时间属性列的任何关系。
         *  timecol：是一个列描述符，指示数据的哪个时间属性列应映射到翻滚窗口。
         *  step：是指定累积窗口的最大宽度的持续时间。size 必须是 step 的整数倍。
         *  size：指定CUMULATE窗口宽度的持续时间。
         *  offset：是一个可选参数，用于指定窗口起点将偏移的偏移量。
         *
         *  Window Offset
         *  偏移是一个可选参数，可用于更改窗口分配。它可以是正持续时间和负持续时间。窗口偏移的默认值为0。
         *  如果设置了不同的偏移值，则相同的记录可能会分配给不同的窗口。
         *
         *
         *
         */
        Table sqlTVF = tableEnv.sqlQuery("select id,window_start, window_end, count(id) as cnt, avg(temperature) as avgTemp " +
                " from TABLE(TUMBLE(TABLE sensor, DESCRIPTOR(ts), INTERVAL '10' seconds)) group by id, window_start, window_end");

        tableEnv.toDataStream(sqlTVF, Row.class).print("sqlTVF group>>>>>");

        env.execute();
    }
}
