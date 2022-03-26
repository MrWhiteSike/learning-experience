package com.bsk.flink.order_pay;

import com.bsk.flink.beans.OrderEvent;
import com.bsk.flink.beans.OrderResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

/**
 * CEP 虽然更加简洁，但是ProcessFunction能控制的细节操作更多
 *
 * CEP还是比较适合事件之间有复杂联系的场景
 *
 * ProcessFunction 用来处理每个独立且靠状态就能联系的事件，灵活性更高。
 *
 */
public class OrderTimeoutWithoutCep {
    // 定义超时事件的侧输出流标签
    private final static OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("order-timeout"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        URL resource = OrderTimeoutWithoutCep.class.getResource("/OrderLog.csv");
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

        // 定义自定义处理函数，主流输出正常匹配订单事件，侧输出流输出超时报警事件
        SingleOutputStreamOperator<OrderResult> resultStream = dataStream
                .keyBy(OrderEvent::getOrderId)
                .process(new OrderPayMatchDetect());

        resultStream.print("pay-normally");
        resultStream.getSideOutput(orderTimeoutTag).print("order-timeout");


        env.execute("order pay timeout without cep");
    }

    // 实现自定义处理函数
    public static class OrderPayMatchDetect extends KeyedProcessFunction<Long, OrderEvent, OrderResult> {
        // 定义状态，保存之前订单是否已经来过create、pay的事件
        ValueState<Boolean> isPayedState;
        ValueState<Boolean> isCreatedState;

        // 定义状态，保存定时器时间戳
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            isPayedState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("pay", Boolean.class, false));
            isCreatedState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("create", Boolean.class, false));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void processElement(OrderEvent orderEvent, KeyedProcessFunction<Long, OrderEvent, OrderResult>.Context context, Collector<OrderResult> collector) throws Exception {
            // 获取状态的值
            Boolean isPayed = isPayedState.value();
            Boolean isCreated = isCreatedState.value();
            Long timerTs = timerTsState.value();

            // 判断当前事件类型
            if ("create".equals(orderEvent.getEventType())){
                // 1. 如果来的是create，要判断是否支付过
                if (isPayed){
                    // 1.1 如果已经正常支付，输出正常匹配结果
                    collector.collect(new OrderResult(orderEvent.getOrderId(), "payed successfully"));
                    // 清空状态，删除定时器
                    isCreatedState.clear();
                    isPayedState.clear();
                    timerTsState.clear();
                    context.timerService().deleteEventTimeTimer(timerTs);
                }else {
                    // 1.2 如果没有支付过，注册15分钟后的定时器，开始等待支付事件
                    Long ts = (orderEvent.getTimestamp() + 15 * 60) * 1000L;
                    context.timerService().registerEventTimeTimer(ts);
                    // 更新状态
                    timerTsState.update(ts);
                    isCreatedState.update(true);
                }
            }else {
                // 2. 如果来的是pay，要判断是否有下单事件来过
                if (isCreated){
                    // 2.1 如果下单事件来过，继续判断支付的时间戳是否超过15分钟
                    if (orderEvent.getTimestamp() * 1000L < timerTs){
                        // 2.1.1 在15分钟内，没有超时，正常匹配输出
                        collector.collect(new OrderResult(orderEvent.getOrderId(), "payed successfully"));
                    }else {
                        // 2.1.2 已经超时，输出侧输出流报警
                        context.output(orderTimeoutTag, new OrderResult(orderEvent.getOrderId(), "payed but already timeout"));
                    }
                    // 统一清空状态
                    isCreatedState.clear();
                    isPayedState.clear();
                    timerTsState.clear();
                    context.timerService().deleteEventTimeTimer(timerTs);
                }else {
                    // 2.2 如果没有下单事件，乱序，注册一个定时器，等待下单事件
                    context.timerService().registerEventTimeTimer(orderEvent.getTimestamp() * 1000L);
                    // 更新状态
                    timerTsState.update(orderEvent.getTimestamp() * 1000L);
                    isPayedState.update(true);
                }
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, OrderEvent, OrderResult>.OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
            // 定时器没来，说明一定有一个事件没来
            if (isPayedState.value()){
                // 如果pay来了，说明create没来
                ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(), "payed but not found created log"));
            }else {
                // create 来了，说明pay没来
                ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(), "timeout"));
            }

            // 清空状态
            isCreatedState.clear();
            isPayedState.clear();
            timerTsState.clear();
        }
    }

}
