package com.bsk.flink.pvuv;

import com.bsk.flink.beans.PageViewCount;
import com.bsk.flink.beans.UserBehavior;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * Created by baisike on 2022/3/21 3:05 下午
 */
public class UniqueVisitor {
    public static void main(String[] args) throws Exception {
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

        // 开窗统计UV
        SingleOutputStreamOperator<PageViewCount> apply = userBehaviorDataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .apply(new UvCountResult());
        apply.print();

        // 执行
        env.execute("page view job");
    }

    /**
     * 这个例子中，我们把所有数据的userID都存在了窗口计算的状态里，在窗口收集数据的过程中，
     * 状态会不断增大，一般情况下，只要不超出内存的承受范围，这种做法没什么问题，但是如果我们遇到、
     * 的数据量很大呢？
     *
     * 把所有数据暂存放到内存里，显然不是一个好主意。我们会想到，可以利用redis
     * 这种内存级k-v数据库，为我们做一个缓存，但是我们遇到情况比较极端，数据大到惊人呢？
     * 比如上亿级的用户，要去重计算uv
     *
     * 如果放到redis中，亿级用户id（每个20字节左右的话），可能需要几G甚至几十G的空间来存储。
     * 当然放到redis中，用集群进行扩展也不是不可以，但明显代价太大。
     *
     * 一个更好的想法是，其实我们不需要完整地存储用户id信息，只要知道他在不在就行了。所以其实我们
     * 可以进行压缩处理，用一位bit就可以表示一个用户的状态。这个思想的具体实现就是布隆过滤器（Bloom Filter）。
     *
     * 本质上布隆过滤器是一种数据结构，比较巧妙的概率型数据结构，特点是高效插入和查询，
     * 可以用来告诉你 某样东西一定存在，或者可能存在
     *
     * 它本身是一个很长的二进制向量，既然是二进制的向量，那么显而易见的，存放
     * 的不是0，就是1，相比于传统的List， Set ，Map等数据结构，它更高效、占用空间
     * 更少，但是缺点是其返回的结果是概率性的，而不是确切的。
     *
     * 我们的目标是：利用某种方法（一般是Hash函数）把每个数据，对应到一个位图
     * 的某一位上去，如果数据存在，那个位上就是1，不存在则为0。
     *
     */
    public static class UvCountResult implements AllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {
        @Override
        public void apply(TimeWindow timeWindow, Iterable<UserBehavior> iterable, Collector<PageViewCount> collector) throws Exception {
            // 去重：定义一个Set结构，保存窗口中的所有userId，自动去重
            HashSet<Long> uidSet = new HashSet<>();
            for (UserBehavior ub: iterable) {
                uidSet.add(ub.getUserId());
            }
            collector.collect(new PageViewCount("uv", timeWindow.getEnd(), (long)uidSet.size()));
        }
    }
}
