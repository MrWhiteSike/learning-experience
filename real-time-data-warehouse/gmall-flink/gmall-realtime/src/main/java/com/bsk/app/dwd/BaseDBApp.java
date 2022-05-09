package com.bsk.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.bsk.app.function.CustomDeserialization;
import com.bsk.app.function.TableProcessFunction;
import com.bsk.bean.TableProcess;
import com.bsk.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.1 设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop1:8020/gmall/dwd_log/ck"));
        // 1.2 开启ck
//        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 2. 消费kafka数据 ods_base_db  主题数据创建流
        String topic = "ods_base_db";
        String groupId = "ods_db_group";
        DataStreamSource<String> kafkaStream = env.addSource(MyKafkaUtil.getKafkaSource(topic, groupId));
        // 3. 将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonStream = kafkaStream.map(JSON::parseObject);
        // 4. 过滤掉空值数据 （过滤delete 数据） 主流
        SingleOutputStreamOperator<JSONObject> filterStream = jsonStream.filter((FilterFunction<JSONObject>) jsonObject -> {
            String type = jsonObject.getString("type");
            return !"delete".equals(type);
        });
        // 5. 打印测试
        filterStream.print();
        // 6.使用FlinkCDC消费配置表 并处理成     广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop1")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-realtime") // 数据库要开启bin_log
                .tableList("gmall-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomDeserialization())
                .build();
        DataStreamSource<String> tableProcessStream = env.addSource(sourceFunction);
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessStream.broadcast(mapStateDescriptor);
        // 7.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectStream = jsonStream.connect(broadcastStream);

        // 8.分流处理：广播流数据，主流数据（根据广播流数据进行处理）
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag") {
        };
        SingleOutputStreamOperator<JSONObject> kafka = connectStream.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));

        // 9.提取kafka流数据 和 HBase流数据
        DataStream<JSONObject> hbase = kafka.getSideOutput(hbaseTag);

        // 10.将kafka数据写入Kafka主题，将HBase数据写入Phoenix表
        kafka.print("Kafka>>>>>>>>>>>>");
        hbase.print("HBase>>>>>>>>>>>>");

        // 11. 执行任务
        env.execute("BaseDBApp");
    }
}
