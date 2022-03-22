package com.bsk.flink.appmarketing_analysis;

import com.bsk.flink.beans.AdClickEvent;
import com.bsk.flink.beans.AdCountViewByProvince;
import com.bsk.flink.beans.BlackListUserWarning;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.sql.Timestamp;
import java.util.Date;

/**
 *  基本需求
 *      从埋点日志中，统计每小时页面广告的点击量，5分钟刷新一次，并按照不同省份进行划分
 *      对于"刷单"式的频繁点击行为进行过滤，并将该用户加入黑名单
 * 解决思路
 *      根据省份进行分组，创建长度为1小时、滑动距离为5分钟的时间窗口进行统计
 *      可以用process function进行黑名单过滤，检测用户对同一广告的点击量，如果超过上限则将用户信息以侧输出流输出到黑名单中
 */

public class AdStatisticsByProvince {
    public static void main(String[] args) throws Exception{
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据
        URL resource = AdStatisticsByProvince.class.getResource("/AdClickLog.csv");
        SingleOutputStreamOperator<AdClickEvent> adClickDataStream = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new AdClickEvent(new Long(fields[0]), new Long(fields[1]), fields[2], fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(new BoundedOutOfOrdernessTimestampExtractor<AdClickEvent>(Time.milliseconds(200)) {
                    @Override
                    public long extractTimestamp(AdClickEvent adClickEvent) {
                        return adClickEvent.getTimestamp() * 1000L;
                    }
                }));

        // 对同一个用户点击同一个广告的行为进行检测报警
        SingleOutputStreamOperator<AdClickEvent> filterDataStream = adClickDataStream
                .keyBy((KeySelector<AdClickEvent, Tuple2<Long, Long>>) adClickEvent -> new Tuple2<>(adClickEvent.getUserId(), adClickEvent.getAdId()))
                .process(new FilterBlackListUser(100));

        // 基于省份分组，开窗聚合
        SingleOutputStreamOperator<AdCountViewByProvince> aggStream = filterDataStream
                .keyBy(AdClickEvent::getProvince)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new AdCountAgg(), new AdCountResult());

        // 主流打印输出
        aggStream.print();
        // 侧输出流打印输出
        filterDataStream.getSideOutput(new OutputTag<BlackListUserWarning>("blacklist"){}).print("blacklist-user");

        // 执行
        env.execute("ad statistics by province job");
    }

    // 过滤同一用户同一个广告的点击超过100的用户利用侧输出流输出到黑名单中
    public static class FilterBlackListUser extends KeyedProcessFunction<Tuple2<Long, Long>, AdClickEvent, AdClickEvent>{
        // 定义属性：点击次数上限
        private Integer countUpperBound;

        public FilterBlackListUser(Integer countUpperBound) {
            this.countUpperBound = countUpperBound;
        }

        // 定义状态，保存当前用户对某一个广告的点击次数
        ValueState<Long> countState;
        // 定义一个标志状态，保存当前用户是否已经被发送到了黑名单里
        ValueState<Boolean> isSentState;

        @Override
        public void open(Configuration parameters) {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
            isSentState = getRuntimeContext().getState(new ValueStateDescriptor<>("isSent", Boolean.class));
        }

        @Override
        public void processElement(AdClickEvent adClickEvent, KeyedProcessFunction<Tuple2<Long, Long>, AdClickEvent, AdClickEvent>.Context context, Collector<AdClickEvent> collector) throws Exception {

            // 判断当前用户对同一广告的点击次数，如果不够上限，该count+1正常输出
            // 如果到达上限，直接过滤掉，并侧输出流输出黑名单报警

            // 首先获取当前count值
            Long curCount = countState.value();
            // 获取是否发送的状态
            Boolean isSent = isSentState.value();

            if (curCount == null){
                curCount = 0L;
            }

            if (isSent == null){
                isSent = false;
            }

            // 1.首先判断是否是第一个数据，如果是的话，注册一个第二天0点的定时器
            if (curCount == 0){
                long ts = context.timerService().currentProcessingTime();
                long fixedTime = DateUtils.addDays(new Date(ts), 1).getTime();
                context.timerService().registerProcessingTimeTimer(fixedTime);
            }

            // 2.判断是否报警
            if (curCount >= countUpperBound){
                // 判断是否输出到黑名单过，如果没有的话，就输出到侧输出流进行报警
                if (!isSent){
                    isSentState.update(true);
                    context.output(new OutputTag<BlackListUserWarning>("blacklist"){},
                            new BlackListUserWarning(adClickEvent.getUserId(), adClickEvent.getAdId(), "click over " + countUpperBound + " times."));
                }
                // 如果当前用户已经在黑名单中，就不在进行下面操作
                return;
            }

            // 如果没有达到报警次数，那么点击次数+1，更新状态，正常输出当前数据到主流
            countState.update(curCount + 1);
            collector.collect(adClickEvent);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) {
            // 清空所有状态
            countState.clear();
            isSentState.clear();
        }
    }


    // 自定义增量聚合函数，预聚合
    public static class AdCountAgg implements AggregateFunction<AdClickEvent, Long, Long>{
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent adClickEvent, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    // 自定义全窗口函数
    public static class AdCountResult implements WindowFunction<Long, AdCountViewByProvince, String, TimeWindow> {
        @Override
        public void apply(String province, TimeWindow timeWindow, Iterable<Long> iterable, Collector<AdCountViewByProvince> collector) {
            collector.collect(new AdCountViewByProvince(province, new Timestamp(timeWindow.getEnd()).toString(), iterable.iterator().next()));
        }
    }

}
