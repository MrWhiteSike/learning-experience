package com.bsk.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
* @ClassName: WordCount
* @Description: 批处理 wordcount
* @Author: baisike on 2022/3/9 10:18 下午
* @Version: 1.0
*/
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 从文件中读取数据
        String inputPath = "/Users/baisike/learning-experience/flink/bigdata-flink/src/main/resources/hello.txt";
        DataSource<String> dataSource = env.readTextFile(inputPath);

        DataSet<Tuple2<String, Integer>> result = dataSource.flatMap(new MyFlatMapper())
                .groupBy(0) // 按照第一个位置进行分组
                .sum(1); // 将第二个位置上的数据求和
        result.print();
    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> out) {
            String[] words = s.split(" ");
            for (String str:words) {
                out.collect(new Tuple2<>(str, 1));
            }
        }
    }
}
