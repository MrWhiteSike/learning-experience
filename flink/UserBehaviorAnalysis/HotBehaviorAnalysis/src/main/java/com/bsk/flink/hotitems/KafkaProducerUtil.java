package com.bsk.flink.hotitems;

/**
 * Created by baisike on 2022/3/20 9:30 上午
 */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

/**
* @ClassName: KafkaProducerUtil
* @Description: Kafka 批量数据测试
* @Author: baisike on 2022/3/20 12:00 下午
* @Version: 1.0
*/
public class KafkaProducerUtil {
    public static void main(String[] args) throws Exception {
        writeToKafka("hotitems");
    }

    public static void writeToKafka(String topic) throws Exception {
        // kafka 配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 定义一个Kafka Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 用缓冲方式来读取文本
        BufferedReader reader = new BufferedReader(new FileReader("/Users/baisike/learning-experience/flink/UserBehaviorAnalysis/HotBehaviorAnalysis/src/main/resources/UserBehavior.csv"));
        String line;
        while ((line = reader.readLine()) != null){
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, line);
            // 用producer发送数据
            kafkaProducer.send(record);
        }
        kafkaProducer.close();
    }
}
