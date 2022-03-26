package com.bsk.flink.order_pay;

import com.bsk.flink.beans.OrderEvent;
import com.bsk.flink.beans.ReceiptEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

import java.net.URL;

/**
 *  join 实现连接：
 *  这种方法的缺陷，只能获得正常匹配的结果，不能获取未匹配成功的记录
 */
public class TxPayMatchByJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取订单支付事件数据
        URL orderResource = TxPayMatchByJoin.class.getResource("/OrderLog.csv");
        SingleOutputStreamOperator<OrderEvent> orderStream = env.readTextFile(orderResource.getPath())
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
                )).filter(data -> !"".equals(data.getTxId()));

        // 读取到账事件数据
        URL receiptResource = TxPayMatchByJoin.class.getResource("/ReceiptLog.csv");
        SingleOutputStreamOperator<ReceiptEvent> receiptStream = env.readTextFile(receiptResource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new ReceiptEvent(fields[0], fields[1], new Long(fields[2]));
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                        new BoundedOutOfOrdernessTimestampExtractor<ReceiptEvent>(Time.milliseconds(200)) {
                            @Override
                            public long extractTimestamp(ReceiptEvent value) {
                                return value.getTimestamp() * 1000L;
                            }
                        }
                ));

        // 区间连接两条流，得到匹配的数据
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> resultStream = orderStream
                .keyBy(OrderEvent::getTxId)
                .intervalJoin(receiptStream.keyBy(ReceiptEvent::getTxId))
                .between(Time.seconds(-3), Time.seconds(5)) // 区间范围：-3，5
                .process(new TxPayMatchDetectByJoin());

        resultStream.print("");

        env.execute("tx match detect by join job");
    }

    public static class TxPayMatchDetectByJoin extends ProcessJoinFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>{

        @Override
        public void processElement(OrderEvent orderEvent, ReceiptEvent receiptEvent, ProcessJoinFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>.Context context, Collector<Tuple2<OrderEvent, ReceiptEvent>> collector) throws Exception {
            collector.collect(new Tuple2<>(orderEvent, receiptEvent));
        }
    }
}
