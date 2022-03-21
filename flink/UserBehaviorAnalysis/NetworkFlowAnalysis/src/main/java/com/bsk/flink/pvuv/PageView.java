package com.bsk.flink.pvuv;

import com.bsk.flink.beans.PageViewCount;
import com.bsk.flink.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by baisike on 2022/3/21 1:32 下午
 */
public class PageView {
    /**
     * PV : Page View 就是网页浏览
     *
     * UV：Unique Visitor 网站的独立访客数
     * 一段时间内访问网站的总人数
     *
     * 1天内同一访客的多次访问只记录一个访客，通过IP和cookie判断UV值的两种方式

     */

    public static void main(String[] args) throws Exception{
        // 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 2.读取数据
        URL resource = PageView.class.getResource("/UserBehavior.csv");
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        // 3.转换成POJO，分配时间戳watermark
        SingleOutputStreamOperator<UserBehavior> userBehaviorDataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.of(200, TimeUnit.MILLISECONDS)) {
                    @Override
                    public long extractTimestamp(UserBehavior userBehavior) {
                        return userBehavior.getTimestamp() * 1000L;
                    }
                }
        ));

        /**
         * 第一种思路：
         * 1、先转换为 固定key 的tuple(key, 1)
         * 2、keyBy分组，所有数据分到一组
         * 3、开窗为1个小时的数据，进行 sum 聚合操作，得到结果
         *
         * 无法进行并行，并且数据倾斜
         *
         *
         */
        SingleOutputStreamOperator<Tuple2<String, Long>> pv = userBehaviorDataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior userBehavior) throws Exception {
                        return new Tuple2<>("pv", 1L);
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .sum(1);


        /**
         * 任务改进：
         *  1、设计随机key，解决数据倾斜问题；
         *  2、修改多次窗口输出的问题，实现同一窗口只输出一次
         */
        SingleOutputStreamOperator<PageViewCount> pvStream = userBehaviorDataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(UserBehavior userBehavior) throws Exception {
                        // 改进1：把数据分到不同组，可以增加并行度，进行分布式计算
                        Random random = new Random();
                        return new Tuple2<>(random.nextInt(10), 1L);
                    }
                })
                .keyBy(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new PvCountAgg(), new PvCountResult());

        SingleOutputStreamOperator<PageViewCount> pvCount = pvStream
                .keyBy(PageViewCount::getWindowEnd)
                // 改进2：修改多次窗口输出的问题，实现同一窗口只输出一次
                .process(new TotalPvCount());
//                .sum("count");

        // 打印输出
        pv.print("");

        pvCount.print("改进后");


        // 执行
        env.execute("page view job");
    }

    // 自定义增量聚合函数，预聚合
    public static class PvCountAgg implements AggregateFunction<Tuple2<Integer, Long>, Long, Long>{
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> integerLongTuple2, Long acc) {
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

    // 自定义窗口函数
    public static class PvCountResult implements WindowFunction<Long, PageViewCount, Integer, TimeWindow>{
        @Override
        public void apply(Integer key, TimeWindow timeWindow, Iterable<Long> iterable, Collector<PageViewCount> collector) throws Exception {
            collector.collect(new PageViewCount(key.toString(), timeWindow.getEnd(), iterable.iterator().next()));
        }
    }

    // 实现自定义处理函数，相同窗口分组统计的count值相加
    public static class TotalPvCount extends KeyedProcessFunction<Long, PageViewCount, PageViewCount>{
        // 定义状态，保存窗口的sum值
        private ValueState<Long> pvCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            pvCountState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("pv-count", Long.class, 0L));
        }

        @Override
        public void processElement(PageViewCount pageViewCount, Context context, Collector<PageViewCount> collector) throws Exception {
            // 获取状态值，注册定时器
            Long count = pvCountState.value();
            pvCountState.update(count + pageViewCount.getCount());
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
            // 定时器触发，所有分组count值都到齐了，直接输出当前总count值
            out.collect(new PageViewCount("pv", ctx.getCurrentKey(), pvCountState.value()));
            pvCountState.clear();
        }
    }
}
