package com.bsk.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bsk.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置ck 以及 状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://192.168.36.121:8020/gmall-flink/ck"));
//        env.enableCheckpointing(5000L); // 生产环境中一般设置为5min 或者 10min；头和头之间的时间间隔
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L); // 生产环境中，根据任务状态大小，进行合理设置
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L); // 两个checkpoint之间最小间隔时间，尾和头之间时间间隔

        // 2. 读取kafka ods_base_log 主题数据
        String sourceTopic = "ods_base_log";
        String groupId = "base_log_app";
        DataStreamSource<String> kafkaStream = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupId));
        // 3. 将每行转换为json对象
        OutputTag<String> outputTag = new OutputTag<String>("Dirty") { // 异常数据侧输出流对象定义，用于输出异常数据
        };
        SingleOutputStreamOperator<JSONObject> jsonStream = kafkaStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    collector.collect(jsonObject);
                } catch (Exception e) {// 捕获数据解析异常
                    // 将异常数据写入侧输出流，比如json字符串缺少 }
                    context.output(outputTag, value);
                }
            }
        });
        // 获取异常数据侧输出流，并输出到 kafka 的 dirty 主题中
        DataStream<String> dirty = jsonStream.getSideOutput(outputTag);
//        dirty.addSink(MyKafkaUtil.getKafkaSink("dirty"));
        dirty.print("Dirty>>>>>>>>>>>>>> ");

        // 4. 新老用户校验 状态编程 ：先分组，再进行首次状态存储，首次为null，不是首次不为null
        KeyedStream<JSONObject, String> keyedStream = jsonStream.keyBy(data -> data.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> jsonNewFlagStream = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            // 声明状态用于表示当前Mid是否已经访问过
            private ValueState<String> firstVisitDateState;
//            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("new-mid", String.class));
//                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                // 只需要处理 is_new = 1 的情况，如果状态为 null，表示首次登录，更新状态值；如果状态不为null，修复为老用户标记
                String isNew = value.getJSONObject("common").getString("is_new");
                if ("1".equals(isNew)) {
                    String firstDate = firstVisitDateState.value();
                    Long ts = value.getLong("ts");
                    if (firstDate != null) {
                        // 老用户登录，修改标记为老用户
                        value.getJSONObject("common").put("is_new", "0");
                    } else {
                        // 新用户登录，更新状态值为新用户的登录时间
//                        firstVisitDateState.update(simpleDateFormat.format(ts));
                        // 随便设置一个值就可以
                        firstVisitDateState.update("1");
                    }
                }
                return value;
            }
        });
//        jsonNewFlagStream.print();
        // 5. 分流 侧输出流 页面：主流  启动：侧输出流  曝光： 侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        SingleOutputStreamOperator<String> pageStream = jsonNewFlagStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
                String startStr = jsonObject.getString("start");
                if (startStr != null && startStr.length() > 0) {
                    // 将启动日志输出到侧输出流
                    context.output(startTag, jsonObject.toString());
                } else {
                    // 页面数据，将数据输出到主流
                    collector.collect(jsonObject.toString());

                    // 判断是否是曝光数据
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        // 将曝光数据遍历写入侧输出流
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject displayJson = displays.getJSONObject(i);
                            // 添加页面ID
                            displayJson.put("page_id", jsonObject.getJSONObject("page").getString("page_id"));
                            context.output(displayTag, displayJson.toString());
                        }
                    }
                }
            }
        });
        // 6. 获取侧输出流的数据
        DataStream<String> startStream = pageStream.getSideOutput(startTag);
        DataStream<String> displayStream = pageStream.getSideOutput(displayTag);
        // 7. 将多个流分别输出到kafka不同的主题中
        pageStream.print("Page>>>>>>>>> ");
        startStream.print("Start>>>>>>>>> ");
        displayStream.print("Display>>>>>>>>> ");
        pageStream.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        startStream.addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        displayStream.addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));

        // 8. 启动任务
        env.execute("BaseLogApp");
    }
}
