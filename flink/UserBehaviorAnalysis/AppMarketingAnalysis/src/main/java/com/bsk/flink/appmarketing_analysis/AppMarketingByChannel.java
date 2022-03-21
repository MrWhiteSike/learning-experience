package com.bsk.flink.appmarketing_analysis;

import com.bsk.flink.beans.ChannelPromotionCount;
import com.bsk.flink.beans.MarketingUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by baisike on 2022/3/21 11:10 下午
 */
public class AppMarketingByChannel {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从自定义数据源中读取数据
        SingleOutputStreamOperator<MarketingUserBehavior> dataStream = env
                .addSource(new SimulatedMarketingUserBehaviorSource())
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(new BoundedOutOfOrdernessTimestampExtractor<MarketingUserBehavior>(Time.milliseconds(200)) {
                    @Override
                    public long extractTimestamp(MarketingUserBehavior value) {
                        return value.getTimestamp();
                    }
                }));

        // 2.分渠道开窗统计
        SingleOutputStreamOperator<ChannelPromotionCount> resultStream = dataStream
                .filter(data -> !"UNINSTALL".equals(data.getBehavior()))
                .keyBy(new KeySelector<MarketingUserBehavior, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(MarketingUserBehavior value) throws Exception {
                        return new Tuple2<>(value.getChannel(), value.getBehavior());
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(5)))
                .aggregate(new MarketingCountAgg(), new MarketingCountResult());

        resultStream.print();
        env.execute("app marketing by channel");
    }

    // 实现自定义模拟市场用户行为数据源
    public static class SimulatedMarketingUserBehaviorSource implements SourceFunction<MarketingUserBehavior>{
        // 控制是否正常运行的标识符
        Boolean running = true;

        // 定义用户行为和渠道的范围
        List<String> behaviorList = Arrays.asList("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL");
        List<String> channelList = Arrays.asList("app store", "wechat", "weibo");

        Random random = new Random();

        @Override
        public void run(SourceContext<MarketingUserBehavior> sourceContext) throws Exception {
            while (running) {
                Long id = random.nextLong();
                String behavior = behaviorList.get(random.nextInt(behaviorList.size()));
                String channel = channelList.get(random.nextInt(channelList.size()));
                Long timestamp = System.currentTimeMillis();
                sourceContext.collect(new MarketingUserBehavior(id, behavior, channel, timestamp));
                Thread.sleep(100L);
            }
        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }

    // 自定义增量聚合函数，预聚合
    public static class MarketingCountAgg implements AggregateFunction<MarketingUserBehavior, Long, Long>{
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MarketingUserBehavior marketingUserBehavior, Long acc) {
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
    public static class MarketingCountResult extends ProcessWindowFunction<Long, ChannelPromotionCount, Tuple2<String, String>, TimeWindow>{
        @Override
        public void process(Tuple2<String, String> tuple2, Context context, Iterable<Long> iterable, Collector<ChannelPromotionCount> collector) throws Exception {
            collector.collect(new ChannelPromotionCount(tuple2.f0, tuple2.f1, new Timestamp(context.window().getEnd()).toString(), iterable.iterator().next()));
        }
    }



}
