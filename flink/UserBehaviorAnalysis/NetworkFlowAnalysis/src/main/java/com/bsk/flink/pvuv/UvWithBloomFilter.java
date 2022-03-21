package com.bsk.flink.pvuv;

import com.bsk.flink.beans.PageViewCount;
import com.bsk.flink.beans.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import redis.clients.util.MurmurHash;

import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Created by baisike on 2022/3/21 3:54 下午
 */
public class UvWithBloomFilter {
    public static void main(String[] args) throws Exception {
        // 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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

        // 利用 Bloom Filter来实现uv去重功能

        // 开窗统计UV
        SingleOutputStreamOperator<PageViewCount> uvStream = userBehaviorDataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .trigger(new MyTrigger())
                .process(new UvCountResultWithBloomFilter());
        uvStream.print();

        env.execute("uv with bloom job");
    }

    // 自定义触发器
    public static class MyTrigger extends Trigger<UserBehavior, TimeWindow>{
        @Override
        public TriggerResult onElement(UserBehavior userBehavior, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            // 每一条数据来到，直接触发窗口计算，并且直接清空窗口
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            // 啥都不做，继续往下执行
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        }
    }

    // 自定义一个布隆过滤器
    public static class MyBloomFilter {
        // 定义位图大小, 一般需要定义2的整次幂
        private Integer cap;

        public MyBloomFilter(Integer cap) {
            this.cap = cap;
        }

        // 实现一个hash函数
        public Long hashCode(String value, Integer seed){
            // 可以采用 MurmurHash 获取hashCode
            Long result = 0L;
            for (int i=0;i<value.length();i++){
               result =  result * seed + value.charAt(i);
            }
            return result & (cap - 1);
        }
    }

    // 自定义的处理函数
    public static class UvCountResultWithBloomFilter extends ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow>{
        // jedis 连接，需要引入redis客户端依赖
        Jedis jedis;
        MyBloomFilter myBloomFilter;



        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("localhost", 6379);
            // 要处理1亿个数据，用64MB大小的位图，利用位移操作
            myBloomFilter = new MyBloomFilter(1<<29);
        }

        @Override
        public void process(Context context, Iterable<UserBehavior> iterable, Collector<PageViewCount> collector) throws Exception {
            // 将位图和窗口count值全部存入redis
            // 用windowEnd作为key
            Long windowEnd = context.window().getEnd();
            String bitmapKey = windowEnd.toString();

            // 把count值存成一张hash表
            String countHashName = "uv_count";
            String countKey = windowEnd.toString();

            // 1.取当前的userId
            Long userId = iterable.iterator().next().getUserId();

            // 2.计算位图中的offset
            Long offset = myBloomFilter.hashCode(userId.toString(), 61);

            // 3. 用redis的getbit 命令，判断对应位置的值
            Boolean isExist = jedis.getbit(bitmapKey, offset);

            if (!isExist){
                // 如果不存在，对应位图位置置为1
                jedis.setbit(bitmapKey, offset, true);

                // 更新redis中保存的count值
                Long uvCount = 0L;
                String uvCountString = jedis.hget(countHashName, countKey);
                if (uvCountString != null && !"".equals(uvCountString)){
                    uvCount = Long.valueOf(uvCountString);
                }
                jedis.hset(countHashName, countKey, String.valueOf(uvCount + 1));

                collector.collect(new PageViewCount("uv", windowEnd, uvCount + 1));
            }
        }

        @Override
        public void close() throws Exception {
            jedis.close();
        }
    }

}
