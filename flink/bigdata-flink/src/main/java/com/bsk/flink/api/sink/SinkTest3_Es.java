package com.bsk.flink.api.sink;

import com.bsk.flink.api.beans.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by baisike on 2022/3/14 4:25 下午
 */
public class SinkTest3_Es {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("/Users/baisike/learning-experience/flink/bigdata-flink/src/main/resources/sensor.txt");

        // 数据转换操作
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],Long.parseLong(fields[1]),Double.parseDouble(fields[2]));
        });

        // 定义es的连接配置
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost",9200));

        dataStream.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts, new MyEsSinkFunction()).build());
        env.execute();
    }

    // 自定义ES写入操作
    public static class MyEsSinkFunction implements ElasticsearchSinkFunction<SensorReading>{
        @Override
        public void process(SensorReading sensor, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {

            //定义写入的数据source
            HashMap<String,String> data = new HashMap<>();
            data.put("id", sensor.getId());
            data.put("temp", sensor.getTemperature().toString());
            data.put("ts", sensor.getTimestamp().toString());

            // 创建请求，作为向es发起的写入命令（ES7统一type就是_doc,不再允许指定type）
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("sensor")
                    .source(data);

            // 用requestIndexer发送请求
            requestIndexer.add(indexRequest);
        }
    }

}
