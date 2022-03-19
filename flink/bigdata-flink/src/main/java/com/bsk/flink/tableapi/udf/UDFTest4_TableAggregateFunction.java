package com.bsk.flink.tableapi.udf;

import com.bsk.flink.api.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * Created by baisike on 2022/3/19 12:52 下午
 */
public class UDFTest4_TableAggregateFunction {
    public static void main(String[] args) throws Exception {

        // 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2.读取数据
        DataStreamSource<String> inputStream = env.readTextFile("/Users/baisike/learning-experience/flink/bigdata-flink/src/main/resources/sensor.txt");

        // 3.转换数据
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        // 4.将流转换成表
        Table dataTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp");

        // 5.自定义聚合函数，求top2的温度值
        // 5.1 table api
        // 需要在环境中注册UDF
        tableEnv.createTemporarySystemFunction("top2", Top2.class);

        Table resultTable = dataTable.groupBy($("id"))
                .flatAggregate(call("top2", $("temp")).as("temp", "rank"))
                .select($("id"), $("temp"), $("rank"));

        // 5.2 SQL还不支持TableAggregateFunction

        // 6.打印输出
        tableEnv.toRetractStream(resultTable, Row.class).print("result");


        env.execute();
    }

    public static class Top2Accumulator{
        public Double first;
        public Double second;
    }

    // 自定义TableAggregateFunction
    public static class Top2 extends TableAggregateFunction<Tuple2<Double, Integer>, Top2Accumulator>{

        @Override
        public Top2Accumulator createAccumulator() {
            Top2Accumulator acc = new Top2Accumulator();
            acc.first = Double.MIN_VALUE;
            acc.second = Double.MIN_VALUE;
            return acc;
        }

        public void accumulate(Top2Accumulator acc, Double value){
            if (value > acc.first){
                acc.second = acc.first;
                acc.first = value;
            } else if (value > acc.second){
                acc.second = value;
            }
        }

        public void merge(Top2Accumulator acc, Iterable<Top2Accumulator> it){
            for (Top2Accumulator otherAcc : it){
                accumulate(acc, otherAcc.first);
                accumulate(acc, otherAcc.second);
            }
        }

        public void emitValue(Top2Accumulator acc, Collector<Tuple2<Double, Integer>> out){
            if (acc.first != Integer.MIN_VALUE){
                out.collect(Tuple2.of(acc.first, 1));
            }
            if (acc.second != Integer.MIN_VALUE){
                out.collect(Tuple2.of(acc.second, 2));
            }
        }
    }
}
