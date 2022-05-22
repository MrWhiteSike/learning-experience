package com.bsk.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.bsk.bean.OrderWide;
import com.bsk.bean.PaymentInfo;
import com.bsk.bean.PaymentWide;
import com.bsk.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class PaymentWideApp {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5));
        // 2. 读取kafka主题数据，生成Javabean对象，提取时间戳生成watermark
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        /**
         * SimpleDateFormat 不能提取出去；因为提取出去 是线程不安全的；如果是创建多个去使用，是没有问题的
         *
         * 可以使用LocalDate LocalTime LocalDateTime 来代替，它们是线程安全的。
         *
         */
        SingleOutputStreamOperator<PaymentInfo> paymentInfoStream = env.addSource(MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId))
                .map(line -> JSONObject.parseObject(line, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo paymentInfo, long l) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(paymentInfo.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return l;
                                }
                            }
                        }));
        SingleOutputStreamOperator<OrderWide> orderWideStream = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId))
                .map(line -> JSONObject.parseObject(line, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide orderWide, long l) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(orderWide.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return l;
                                }
                            }
                        }));
        // 3. 双流join
        SingleOutputStreamOperator<PaymentWide> paymentWideStream = paymentInfoStream.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideStream.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });
        // 4. 将数据写入kafka主题
        paymentWideStream.print("paymentWideStream >>>>>>>>>>>");
        paymentWideStream
                .map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));
        // 5. 启动执行
        env.execute("PaymentWideApp");

    }
}
