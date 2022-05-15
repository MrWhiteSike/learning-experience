package com.bsk.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.bsk.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;


public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.1 设置状态后端 && ck
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop1:8020/gmall/dwm_log/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5));

        // 2.读取kafka dwd_page_log主题中数据
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> kafkaSourceStream = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupId));

        // 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonStream = kafkaSourceStream.map(JSON::parseObject);

        // 4.过滤数据，状态编程 ：只保留每个mid每天第一次登录的数据
        KeyedStream<JSONObject, String> keyedStream = jsonStream.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> uvStream = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> dateState;
            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("date-state", String.class);
                // 为状态设置TTL，即给状态设置一个超时时间，过了超时时间就会清空状态
                // 以及更新超时时间的方式
                StateTtlConfig stateTtlConfig = new StateTtlConfig
                        .Builder(Time.hours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 当 创建和更新状态的时候，为状态设置一个新的超时时间
                        .build();
                valueStateDescriptor.enableTimeToLive(stateTtlConfig);
                dateState = getRuntimeContext().getState(valueStateDescriptor);
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                // 取出上一跳页面信息
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                // 判断上一跳页面是否为null
                if (lastPageId == null || lastPageId.length()<=0){
                    // 取出状态数据
                    String lastDate = dateState.value();
                    // 取出当天日期
                    String currDate = simpleDateFormat.format(value.getLong("ts"));
                    if(!currDate.equals(lastDate)){
                        dateState.update(currDate);
                        return true;
                    }
//                    else {
//                        return false;
//                    }
                }
                //else {
                    return false;
                //}
            }
        });
        // 5.将数据写入kafka
        uvStream.print();
        uvStream.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        // 6.启动任务
        env.execute("UniqueVisitApp");
    }
}
