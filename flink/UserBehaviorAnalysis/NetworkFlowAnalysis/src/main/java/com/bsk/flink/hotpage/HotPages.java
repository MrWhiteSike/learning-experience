package com.bsk.flink.hotpage;

import com.bsk.flink.beans.ApacheLogEvent;
import com.bsk.flink.beans.PageViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by baisike on 2022/3/20 5:40 下午
 */
public class HotPages {

    /**
     测试数据：
     watermark延迟：1s & 窗口大小：10min ，滑动步长：5s；定时器触发：windowEnd + 1s; 允许延迟：1min

     83.149.9.216 - - 17/05/2015:10:25:49 +0000 GET /presentations/  => [10:15:50,10:25:50) watermark:10:25:48
     83.149.9.216 - - 17/05/2015:10:25:50 +0000 GET /presentations/  => [10:15:55,10:25:55) watermark:10:25:49
     83.149.9.216 - - 17/05/2015:10:25:51 +0000 GET /presentations/  => [10:15:55,10:25:55) watermark:10:25:50 >= windowEnd10:25:50 ==> 触发一次窗口[10:15:50,10:25:50)计算
     83.149.9.216 - - 17/05/2015:10:25:52 +0000 GET /presentations/  => [10:15:55,10:25:55) watermark:10:25:51 >= windowEnd10:25:50 + 1 ==> 触发定时器
     83.149.9.216 - - 17/05/2015:10:25:55 +0000 GET /presentations/  => [10:16:00,10:26:00) watermark:10:25:54
     83.149.9.216 - - 17/05/2015:10:25:56 +0000 GET /presentations/  => [10:16:00,10:26:00) watermark:10:25:55 >= windowEnd10:25:55 ==> 触发一次窗口[10:15:55,10:25:55)计算
     83.149.9.216 - - 17/05/2015:10:25:56 +0000 GET /present   => [10:16:00,10:26:00)  watermark:10:25:55 (这条数据watermark没有改变，watermark只有改变的是时候，才会触发窗口计算)
     83.149.9.216 - - 17/05/2015:10:25:57 +0000 GET /present  => [10:16:00,10:26:00) watermark:10:25:56 >= windowEnd10:25:55 + 1 ==> 触发定时器
     83.149.9.216 - - 17/05/2015:10:26:01 +0000 GET /  => [10:26:05,10:26:05) watermark:10:26:00 >= windowEnd10:26:00 触发窗口[10:16:00,10:26:00)计算
     83.149.9.216 - - 17/05/2015:10:26:02 +0000 GET /pre  => [10:26:05,10:26:05) watermark:10:26:01 >=  watermark:10:26:00 + 1 ==> 触发定时器
     83.149.9.216 - - 17/05/2015:10:25:46 +0000 GET /presentations/  => [10:15:50,10:25:50) - [10:25:45,10:35:45) 迟到数据并且在允许迟到的1min之内 ==> 触发窗口[10:15:50,10:25:50),[10:15:55,10:25:55),[10:16:00,10:26:00)的计算
     83.149.9.216 - - 17/05/2015:10:26:03 +0000 GET /pre  => [10:16:05,10:26:05) watermark:10:26:02 ==> 触发定时器


     */


    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取文件，转换为POJO
//        URL resource = HotPages.class.getResource("/apache.log");
//        DataStreamSource<String> inputStream = env.readTextFile(resource.getPath());

        // 本地文本流
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 9999);


        SingleOutputStreamOperator<ApacheLogEvent> dataStream = inputStream.map(line -> {
            String[] fields = line.split(" ");
            SimpleDateFormat format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            long timestamp = format.parse(fields[3]).getTime();
            return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                // 设置 watermark 延迟为 1s
                new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(ApacheLogEvent apacheLogEvent) {
                        return apacheLogEvent.getTimestamp();
                    }
                }
        ));

        dataStream.print("data");

        // 分组开窗聚合
        // 定义一个侧输出流标签
        OutputTag<ApacheLogEvent> lateTag = new OutputTag<ApacheLogEvent>("late"){};

        SingleOutputStreamOperator<PageViewCount> windowStream = dataStream
                .filter(data -> "GET".equals(data.getMethod()))
                .filter(data -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                })
                .keyBy(ApacheLogEvent::getUrl)
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                // 延迟数据的第二重保障：允许再延迟一段时间；每来一条，计算一次
                .allowedLateness(Time.minutes(1))
                // 延迟数据的第三重保障：侧输出流接收更加延迟的数据
                /**
                 * 注意：一个数据只有不属于任何没有关闭的窗口了，才会被丢进侧输出流
                 * 即，一个数据所属的窗口都关闭了，才会丢进侧输出流
                 */
                .sideOutputLateData(lateTag)
                .aggregate(new PageCountAgg(), new PageCountResult());

        windowStream.print("window");
        windowStream.getSideOutput(lateTag).print("late");

        // 收集同一窗口count数据， 排序 取top n输出
        SingleOutputStreamOperator<String> process = windowStream
                .keyBy(PageViewCount::getWindowEnd)
                .process(new TopNPages(3));

        // 打印输出
        process.print();

        // 执行
        env.execute("hot page analysis");
    }

    // 自定义增量聚合函数，预聚合
    public static class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long>{
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent apacheLogEvent, Long acc) {
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

    // 自定义全窗口函数统计窗口
    public static class PageCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow>{
        @Override
        public void apply(String url, TimeWindow timeWindow, Iterable<Long> iterable, Collector<PageViewCount> collector) throws Exception {
            Long windowEnd = timeWindow.getEnd();
            Long count = iterable.iterator().next();
            collector.collect(new PageViewCount(url, windowEnd, count));
        }
    }

    //

    public static class TopNPages extends KeyedProcessFunction<Long, PageViewCount, String>{
        // 私有属性
        private Integer topSize;

        public TopNPages(Integer topSize) {
            this.topSize = topSize;
        }

        // 定义状态，保存当前所有PageViewCount到 list中
        // 保存list有问题：改为保存到map中
//        private ListState<PageViewCount> pageViewCountListState;
        private MapState<String, Long> pageViewCountMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
//            pageViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount>("hot-page-list", PageViewCount.class));
            pageViewCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("hot-page-map", String.class, Long.class));
        }

        @Override
        public void processElement(PageViewCount pageViewCount, Context context, Collector<String> collector) throws Exception {
//            pageViewCountListState.add(pageViewCount);
            pageViewCountMapState.put(pageViewCount.getUrl(), pageViewCount.getCount());
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 1);

            // 注册一个 1分钟后的定时器，用来清空状态
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 60 * 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 先判断是否到了窗口关闭清理时间，如果是，直接清空状态返回
            if (timestamp == ctx.getCurrentKey() + 60 * 1000L){
                pageViewCountMapState.clear();
                return;
            }

            // 排序
//            ArrayList<PageViewCount> pageViewCounts = Lists.newArrayList(pageViewCountListState.get().iterator());
            ArrayList<Map.Entry<String, Long>> pageViewCounts = Lists.newArrayList(pageViewCountMapState.entries().iterator());
//            pageViewCounts.sort((a,b) -> -Long.compare(a.getCount(), b.getCount()));
            pageViewCounts.sort((a,b) -> -Long.compare(a.getValue(), b.getValue()));
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("=========================").append(System.lineSeparator());
            stringBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append(System.lineSeparator());
            // 取top n
            for (int i = 0; i < Math.min(topSize, pageViewCounts.size()); i++){
                stringBuilder.append("No ").append(i+1)
//                        .append("页面Url = ").append(pageViewCounts.get(i).getUrl())
                        .append("页面Url = ").append(pageViewCounts.get(i).getKey())
//                        .append("浏览量 = ").append(pageViewCounts.get(i).getCount())
                        .append("浏览量 = ").append(pageViewCounts.get(i).getValue())
                        .append(System.lineSeparator());
            }

            stringBuilder.append("=========================").append(System.lineSeparator());

            Thread.sleep(1000L);
            out.collect(stringBuilder.toString());
            // 直接清空，可以解决刷榜的问题；这样直接清空，导致前面几次排名信息中丢失第一名以外的数据的问题
//            pageViewCountListState.clear();
        }
    }
}
