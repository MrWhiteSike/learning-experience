package com.bsk.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.bsk.app.function.CustomDeserialization;
import com.bsk.app.function.DimSinkFunction;
import com.bsk.app.function.TableProcessFunction;
import com.bsk.bean.TableProcess;
import com.bsk.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;


// 数据流： web/app（客户端） -> nginx（负载均衡，反向代理） -> springboot服务 -> MySql -> FlinkApp -> kafka(ods) -> FlinkApp -> kafka(DWD)/Phoenix(DIM)
// 程 序：                                                      MockDB.jar -> Mysql ->  FlinkCDC -> Kafka(zk) -> BaseDBApp -> kafka(zk)/ Phoenix(hbase,zk,hdfs)
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
//        filterStream.print();
        // 6.使用FlinkCDC消费配置表 并处理成     广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
//                .hostname("192.168.36.120")
                .hostname("hadoop1")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-realtime") // 数据库要开启bin_log
                .tableList("gmall-realtime.table_process")
                .deserializer(new CustomDeserialization())
                .startupOptions(StartupOptions.initial())
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

        // 将HBase数据写入Phoenix表
        /**
         * 分析：
         * 这里可以使用JdbcSink.sink()吗？
         * 不可以，因为第一个参数：SQL语句中的字段数量不确定，第二个参数就无法进行 传参
         * 解决办法：自定义sinkFunction
         *
         */
        hbase.addSink(new DimSinkFunction());

        // 将数据写入到kafka中
        // 泛型：方法内部的类型不确定，不能在方法内部进行逻辑的编写；那么就只能利用参数将在方法外部创建的具体的对象传递到方法内部；即谁用谁传具体的类型，然后写逻辑
        kafka.addSink(MyKafkaUtil.getKafkaSink(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(element.getString("sinkTable"), element.getString("after").getBytes());
            }
        }));

        // 11. 执行任务
        env.execute("BaseDBApp");
    }
}
