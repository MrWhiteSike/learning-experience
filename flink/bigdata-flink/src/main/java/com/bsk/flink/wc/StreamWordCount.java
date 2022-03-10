package com.bsk.flink.wc;

/**
 * Created by baisike on 2022/3/10 6:08 下午
 */

import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: StreamWordCount
 * @Description:
 * @Author: baisike on 2022/3/10 6:09 下午
 * @Version: 1.0
 */
public class StreamWordCount {

    /**
    * @Description:
    * @Param: [args]
    * @return: void
    * @Author: baisike
    * @Date: 2022/3/10 6:09 下午
    */
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setParallelism(1);

        // 从文件中读取数据
//        String inputPath = "/Users/baisike/learning-experience/flink/bigdata-flink/src/main/resources/hello.txt";
//        DataStreamSource<String> dataStream = executionEnvironment.readTextFile(inputPath);

        // netcat: nc -lk 9999

        // 用 parameter tool 工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");


        // 从socket文本流读取数据
        DataStreamSource<String> dataStream = executionEnvironment.socketTextStream(host, port);


        // 基于数据流进行转换计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = dataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);

        // 打印输出
        result.print();

        // 执行任务
        executionEnvironment.execute();
    }
}
