package com.bsk.flink.cdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.1 开启checkpoint，并指定状态后端为Fs   memory fs rocksdb
        env.setStateBackend(new FsStateBackend("hdfs://192.168.36.121:8020/gmall-flink/ck"));
        env.enableCheckpointing(5000L); // 生产环境中一般设置为5min 或者 10min；头和头之间的时间间隔
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L); // 生产环境中，根据任务状态大小，进行合理设置
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L); // 两个checkpoint之间最小间隔时间，尾和头之间时间间隔
        // 1.12 新版本之后不用设置这个参数，已经默认的重启的参数设置的比较合理；老版本（1.10及以下）中必须要设置这个参数
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5));
        // 2.通过FlinkCDC构建SourceFunction并读取数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("192.168.36.121")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-flink")
                .tableList("gmall-flink.base_trademark") // 如果不添加该参数，则消费指定数据库中所有表的数据。如果指定，指定方式为db.tablename
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);
        // 3.打印数据
        streamSource.print();
        // 4.启动任务
        env.execute();
    }
}
