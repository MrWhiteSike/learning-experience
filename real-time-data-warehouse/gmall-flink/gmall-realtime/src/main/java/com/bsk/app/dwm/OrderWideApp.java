package com.bsk.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bsk.app.function.DimAsyncFunction;
import com.bsk.bean.OrderDetail;
import com.bsk.bean.OrderInfo;
import com.bsk.bean.OrderWide;
import com.bsk.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1. 获取执行环境
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
        //TODO 2. 读取kafka主题的数据 转换为JavaBean对象 & 提取时间戳生成watermark
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";
        SingleOutputStreamOperator<OrderInfo> orderStream = env.addSource(MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId))
                .map(line -> {
                    OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);
                    String create_time = orderInfo.getCreate_time();
                    String[] dateTimeArr = create_time.split(" ");
                    orderInfo.setCreate_date(dateTimeArr[0]);
                    orderInfo.setCreate_hour(dateTimeArr[1]);
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderInfo.setCreate_ts(simpleDateFormat.parse(create_time).getTime());
                    return orderInfo;
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo orderInfo, long l) {
                        return orderInfo.getCreate_ts();
                    }
                }));
        SingleOutputStreamOperator<OrderDetail> detailStream = env.addSource(MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId))
                .map(line -> {
                    OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderDetail.setCreate_ts(simpleDateFormat.parse(orderDetail.getCreate_time()).getTime());
                    return orderDetail;
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail orderDetail, long l) {
                        return orderDetail.getCreate_ts();
                    }
                }));
        //TODO 3. 双流join
        SingleOutputStreamOperator<OrderWide> orderWideStream = orderStream.keyBy(OrderInfo::getId)
                .intervalJoin(detailStream.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5)) // 不丢数据：生产环境中给最大的延迟时间
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });
        // 打印测试 : 要开启的服务 ：hdfs、zk、kafka、hbase、MySQL、FlinkCDC、BaseDBApp、OrderWideApp、gmall-mock-db
        orderWideStream.print("orderWideStream>>>>>>>>>> ");

        //TODO 4. 关联维度信息  数据在HBase Phoenix去查 建立connect

        // 关联用户维度
        // 根据user_id 查询Phoenix用户信息
        // 将用户信息补充orderwide
        // 返回结果

        // 其他维度：地区，SKU，SPU ... 维度； ===> 提取一个查询Phoenix的工具类，提高代码复用性

        // 访问Phoenix的时候，先访问zk，超时时间为60s，这里的超时时间至少为60s
        // 4.1 关联用户维度:
        SingleOutputStreamOperator<OrderWide> orderWideWithUserStream = AsyncDataStream.unorderedWait(
                orderWideStream,
                // 利用抽象方法来解决泛型问题： 写框架，工具类的时候，会用泛型，抽象方法很好用！
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setUser_gender(dimInfo.getString("GENDER"));
                        // 生日算年龄
                        String birthday = dimInfo.getString("BIRTHDAY");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        long currentTs = System.currentTimeMillis();
                        long ts = sdf.parse(birthday).getTime();
                        long age = (currentTs - ts) / (1000 * 60 * 60 * 24 * 365L);
                        orderWide.setUser_age((int) age);
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        // 打印测试
//        orderWideWithUserStream.print("orderWideWithUserStream >>>>>>>>>>> ");

        // 4.2 关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceStream = AsyncDataStream.unorderedWait(
                orderWideWithUserStream,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setProvince_name(dimInfo.getString("NAME"));
                        orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // 4.3 关联SKU维度 : 这个一定在下边三个维度关联之前
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuStream = AsyncDataStream.unorderedWait(
                orderWideWithProvinceStream,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setSku_name(dimInfo.getString("SKU_NAME"));
                        orderWide.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(dimInfo.getLong("SPU_ID"));
                        orderWide.setTm_id(dimInfo.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSku_id().toString();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        // 4.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuStream = AsyncDataStream.unorderedWait(
                orderWideWithSkuStream,
                new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setSpu_name(dimInfo.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSpu_id().toString();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // 4.5 关联trademark维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmStream = AsyncDataStream.unorderedWait(
                orderWideWithSpuStream,
                new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setTm_name(dimInfo.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getTm_id().toString();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // 4.6 关联Category3维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3Stream = AsyncDataStream.unorderedWait(
                orderWideWithTmStream,
                new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setCategory3_name(dimInfo.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getCategory3_id().toString();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        orderWideWithCategory3Stream.print("orderWideWithCategory3Stream >>>>>>>> ");
        //TODO 5.将数据写入kafka
        orderWideWithCategory3Stream
                .map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));
        //TODO 6.启动任务
        env.execute("OrderWideApp");
    }
}
