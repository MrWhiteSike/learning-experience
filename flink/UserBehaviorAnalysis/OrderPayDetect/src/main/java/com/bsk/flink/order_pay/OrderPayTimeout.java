package com.bsk.flink.order_pay;

import com.bsk.flink.beans.OrderEvent;
import com.bsk.flink.beans.OrderResult;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * 订单支付超时
 */
public class OrderPayTimeout {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        URL resource = OrderPayTimeout.class.getResource("/OrderLog.csv");
        SingleOutputStreamOperator<OrderEvent> dataStream = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                        new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.milliseconds(200)) {
                            @Override
                            public long extractTimestamp(OrderEvent orderEvent) {
                                return orderEvent.getTimestamp() * 1000L;
                            }
                        }
                ));

        // 1. 定义一个带时间限制的cep模式
        Pattern<OrderEvent, OrderEvent> orderPayPattern = Pattern.<OrderEvent>begin("create").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent) throws Exception {
                return "create".equals(orderEvent.getEventType());
            }
        }).followedBy("pay").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent) throws Exception {
                return "pay".equals(orderEvent.getEventType());
            }
        }).within(Time.minutes(5));

        // 2. 定义侧输出流标签，用来表示超时事件
        OutputTag<OrderResult> outputTag = new OutputTag<OrderResult>("order-timeout") {
        };

        // 3. 将pattern应用到输入数据上，得到pattern stream
        PatternStream<OrderEvent> patternStream = CEP.pattern(dataStream.keyBy(OrderEvent::getOrderId), orderPayPattern);

        // 4. 调用select方法，实现对匹配复杂事件和超时复杂事件的提取和处理
        SingleOutputStreamOperator<OrderResult> resultStream = patternStream.select(outputTag, new OrderTimeoutSelect(), new OrderPaySelect());

        resultStream.print();
        resultStream.getSideOutput(outputTag).print("order-timeout");

        env.execute("order pay timeout job");
    }

    // 实现自定义的超时事件处理函数
    public static class OrderTimeoutSelect implements PatternTimeoutFunction<OrderEvent, OrderResult> {

        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
            Long orderId = pattern.get("create").iterator().next().getOrderId();
            return new OrderResult(orderId, "timeout " + timeoutTimestamp);
        }
    }

    // 实现自定义的正常匹配事件处理函数
    public static class OrderPaySelect implements PatternSelectFunction<OrderEvent, OrderResult>{

        @Override
        public OrderResult select(Map<String, List<OrderEvent>> pattern) throws Exception {
            Long orderId = pattern.get("pay").iterator().next().getOrderId();
            return new OrderResult(orderId, "payed");
        }
    }
}
