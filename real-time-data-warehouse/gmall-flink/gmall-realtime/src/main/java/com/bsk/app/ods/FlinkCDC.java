package com.bsk.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.bsk.app.function.CustomDeserialization;
import com.bsk.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2.通过FlinkCDC构建SourceFunction并读取数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
//                .hostname("192.168.36.121")
                .hostname("hadoop1")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-flink")
                // 需要监控整个库，不需要监控某个表，下边的语句需要注释掉
//                .tableList("gmall-flink.base_trademark")
                .deserializer(new CustomDeserialization())
                // 历史数据就不需要输出了，只输出增量数据就可以了，initial 修改为 latest
                .startupOptions(StartupOptions.latest())
                .build();
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);
        // 3.输出数据到kafka的topic中
        streamSource.print();
        String sinkTopic = "ods_base_db";
        streamSource.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));
        // 4.启动任务
        env.execute("FlinkCDC");
    }
}
