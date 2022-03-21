package com.bsk.flink.appmarketing_analysis;

import com.bsk.flink.beans.ChannelPromotionCount;
import com.bsk.flink.beans.MarketingUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

/**
 * Created by baisike on 2022/3/21 11:50 下午
 */
public class AppMarketingStatistics {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从自定义数据源中读取数据
        DataStream<MarketingUserBehavior> dataStream = env.addSource(new AppMarketingByChannel.SimulatedMarketingUserBehaviorSource())
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                        new BoundedOutOfOrdernessTimestampExtractor<MarketingUserBehavior>(Time.of(200, TimeUnit.MILLISECONDS)) {
                            @Override
                            public long extractTimestamp(MarketingUserBehavior element) {
                                return element.getTimestamp();
                            }
                        }
                ));

        DataStream<ChannelPromotionCount> resultStream = dataStream
                .filter(data -> !"UNINSTALL".equals(data.getBehavior()))
                .map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(MarketingUserBehavior value) throws Exception {
                        return new Tuple2<>("total", 1L);
                    }
                })
                .keyBy(tuple2 -> tuple2.f0)
                // 定义滑窗
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(5)))
                .aggregate(new MarketingStatisticsAgg(), new MarketingStatisticsResult());

        resultStream.print();

        env.execute("app marketing Statistics job");
    }

    public static class MarketingStatisticsAgg implements AggregateFunction<Tuple2<String, Long>, Long, Long>{
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Long> stringLongTuple2, Long acc) {
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

    public static class MarketingStatisticsResult implements WindowFunction<Long, ChannelPromotionCount, String, TimeWindow>{
        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ChannelPromotionCount> collector) throws Exception {
            collector.collect(new ChannelPromotionCount("total", "total", new Timestamp(timeWindow.getEnd()).toString(), iterable.iterator().next() ));
        }
    }
}
