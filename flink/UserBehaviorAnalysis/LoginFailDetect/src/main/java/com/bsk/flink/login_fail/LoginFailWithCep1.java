package com.bsk.flink.login_fail;

import com.bsk.flink.beans.LoginEvent;
import com.bsk.flink.beans.LoginFailWarning;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;

import java.net.URL;
import java.util.List;
import java.util.Map;

public class LoginFailWithCep1 {
    public static void main(String[] args) throws Exception {
        // 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.读取数据，数据格式转换，提取时间戳并生成watermark
        URL resource = LoginFail.class.getResource("/LoginLog.csv");
        SingleOutputStreamOperator<LoginEvent> dataStream = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                        new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
                            @Override
                            public long extractTimestamp(LoginEvent loginEvent) {
                                return loginEvent.getTimestamp() * 1000L;
                            }
                        }
                ));

        // 3. 采用Cep对复杂事件进行处理，利用循环模式进行优化
        // 3.1 定义一个匹配模式
        // firstFail -> secondFail , within 2s
        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern
                .<LoginEvent>begin("failEvents").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent) throws Exception {
                return "fail".equals(loginEvent.getLoginState());
            }
        }).times(3).consecutive()
                .within(Time.seconds(5));

        // 3.2 将匹配模式应用到数据流上，得到一个pattern stream
        PatternStream<LoginEvent> patternStream = CEP.pattern(dataStream.keyBy(LoginEvent::getUserId), loginFailPattern);

        // 3.3  检出符合匹配条件的复杂事件，进行转换处理，得到报警信息
        SingleOutputStreamOperator<LoginFailWarning> resultStream = patternStream.select(new LoginFailMatchDetectWarning());

        resultStream.print();

        env.execute("login fail with cep job");
    }

    // 实现自定义的PatternSelectFunction
    public static class LoginFailMatchDetectWarning implements PatternSelectFunction<LoginEvent, LoginFailWarning> {
        @Override
        public LoginFailWarning select(Map<String, List<LoginEvent>> pattern) throws Exception {
            LoginEvent firstFail = pattern.get("failEvents").get(0);
            LoginEvent lastFailEvent = pattern.get("failEvents").get(pattern.get("failEvents").size() -1);
            return new LoginFailWarning(firstFail.getUserId(), firstFail.getTimestamp(), lastFailEvent.getTimestamp(), "login fail "+pattern.get("failEvents").size()+" times");
        }
    }
}
