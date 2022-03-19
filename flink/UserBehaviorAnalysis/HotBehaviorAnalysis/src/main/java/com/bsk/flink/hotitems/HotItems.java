package com.bsk.flink.hotitems;

import com.bsk.flink.beans.ItemViewCount;
import com.bsk.flink.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by baisike on 2022/3/19 8:30 下午
 */
public class HotItems {
    public static void main(String[] args) throws Exception{
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.从csv文件中读取数据
//        DataStream<String> inputStream = env.readTextFile("/Users/baisike/learning-experience/flink/UserBehaviorAnalysis/HotBehaviorAnalysis/src/main/resources/UserBehavior.csv");


        // 2.从kafka 消费数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer");
        // 次要参数
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<>("hotitems", new SimpleStringSchema(), properties));

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

        // 4.分组开窗聚合，得到每个窗口内各个商品的count值
        DataStream<ItemViewCount> windowAggStream = userBehaviorDataStream
                // 过滤保留pv行为
                .filter(userBehavior -> "pv".equals(userBehavior.getBehavior()))
                // 按照商品ID分组
                .keyBy(UserBehavior::getItemId)
                // 滑动窗口
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                // 增量聚合，统计窗口各个ItemId的count
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());

        // 5.收集同一窗口的所有商品的count数据，排序输出top n
        SingleOutputStreamOperator<String> resultStream = windowAggStream
                // 按照窗口分组
                .keyBy(ItemViewCount::getWindowEnd)
                // 用自定义处理函数排序取前5
                .process(new TopNHotItems(5));

        // 6.打印输出
        resultStream.print();
        // 7、执行
        env.execute("hot items analysis");
    }

    // 实现自定义增量聚合函数
    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long>{
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }
    }

    // 实现全窗口函数
    public static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Long, TimeWindow>{
        @Override
        public void apply(Long itemId, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
            Long windowEnd = timeWindow.getEnd();
            Long count = iterable.iterator().next();
            collector.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }

    //
    public static class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCount, String> {
        // 定义属性，TopN的大小
        private Integer topSize;
        // 定义状态列表，保存当前窗口内所有输出的ItemViewCount
        ListState<ItemViewCount> itemViewCountListState;

        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-view", ItemViewCount.class));

        }

        @Override
        public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
            // 每来一条数据，存入List中，并注定定时器
            itemViewCountListState.add(itemViewCount);
            // 模拟等待，所以这里时间设的比较短（1s）
            context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，当前已收集到的所有数据，排序输出
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());
            // 从多到少（越热门越前面）
            itemViewCounts.sort((a,b)-> -Long.compare(a.getCount(), b.getCount()));
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("=========================").append(System.lineSeparator());
            resultBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append(System.lineSeparator());

            // 遍历列表，取top n 输出
            for (int i=0; i<Math.min(topSize, itemViewCounts.size());i++){
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                resultBuilder.append("NO ").append(i+1).append(":")
                        .append(" 商品ID = ").append(currentItemViewCount.getItemId())
                        .append(" 热门度 = ").append(currentItemViewCount.getCount())
                        .append(System.lineSeparator());
            }
            resultBuilder.append("=========================").append(System.lineSeparator());
            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());
        }
    }
}
