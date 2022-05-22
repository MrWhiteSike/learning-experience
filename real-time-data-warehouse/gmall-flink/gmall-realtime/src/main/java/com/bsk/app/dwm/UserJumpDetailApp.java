package com.bsk.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.bsk.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

// 数据流： web/app（客户端） -> nginx -> springboot服务  -> kafka(ods) -> FlinkApp -> kafka(DWD) -> FlinkApp -> kafka(DWM)
// 程 序：  MockDB.jar   ->   nginx -> logger.sh -> Kafka(zk) -> BaseLogApp -> kafka(zk) -> UserJumpDetailApp -> kafka(zk)
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置状态后端 & ck
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop1:8020/gmall/dwm/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5));

        //TODO 2.读取Kafka主题中的数据创建流
        String groupId = "UserJumpDetailApp";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> kafkaStream = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupId));
        //TODO 3.将每行数据转换为JSON对象并提取时间戳生成Watermark
        SingleOutputStreamOperator<JSONObject> jsonStream = kafkaStream.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long l) {
                        return element.getLong("ts");
                    }
                }));

        //TODO 4.定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                // 获取上一跳信息
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).next("next") // next 是严格近邻
                .where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                // 获取上一跳信息
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).within(Time.seconds(10));

        // 使用循环模式 定义模式序列: .times(2) : 相当于followedBy, 非严格近邻； .times(2).consecutive() : 相当于next, 严格近邻
//        Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject value) throws Exception {
//                // 获取上一跳信息
//                String lastPageId = value.getJSONObject("page").getString("last_page_id");
//                return lastPageId == null || lastPageId.length() <= 0;
//            }
//        })
//                .times(2) // 循环模式
//                .consecutive() // 严格近邻
//                .within(Time.seconds(10));

        //TODO 5.将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(jsonStream.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid")), pattern);

        //TODO 6.提取匹配上的和超时事件
        OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("timeOut") {
        };
        SingleOutputStreamOperator<JSONObject> selectStream = patternStream.select(timeOutTag,
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        // 第一条数据为null，第二条10s内数据没来；获取第一条数据
                        return map.get("start").get(0);
                    }
                }, new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                        // // 第一条数据为null，第二条10s内数据为null也来了；获取第一条数据
                        return map.get("start").get(0);
                    }
                });
        DataStream<JSONObject> timeOutStream = selectStream.getSideOutput(timeOutTag);

        //TODO 7.UNION两种事件
        DataStream<JSONObject> unionStream = selectStream.union(timeOutStream);

        //TODO 8.将数据写入kafka
        unionStream.print();
        unionStream.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        //TODO 9.启动任务
        env.execute("UserJumpDetailApp");
    }
}
