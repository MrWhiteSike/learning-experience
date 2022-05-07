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
        // 设置ck 以及 状态后端：生产环境中必须设置，测试中可以先进行关闭便于测试业务逻辑代码
//        env.setStateBackend(new FsStateBackend("hdfs://192.168.36.121:8020/gmall-flink/ck"));
//        env.enableCheckpointing(5000L); // 生产环境中一般设置为5min 或者 10min；头和头之间的时间间隔
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L); // 生产环境中，根据任务状态大小，进行合理设置
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L); // 两个checkpoint之间最小间隔时间，尾和头之间时间间隔

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
