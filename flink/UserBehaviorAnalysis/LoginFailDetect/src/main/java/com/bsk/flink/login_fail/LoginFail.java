package com.bsk.flink.login_fail;

import com.bsk.flink.beans.LoginEvent;
import com.bsk.flink.beans.LoginFailWarning;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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

import java.net.URL;
import java.util.ArrayList;

public class LoginFail {
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

        // 3.数据根据用户id分组，利用process function对连续2秒内，登录失败次数达到 3 次的进行检测报警
        SingleOutputStreamOperator<LoginFailWarning> resultStream = dataStream
                .keyBy(LoginEvent::getUserId)
                .process(new LoginFailDetectWarning(3));
        // 4.打印输出
        resultStream.print();

        // 5.执行
        env.execute("login fail job");
    }

    // 登录失败次数达到上限时进行输出报警
    public static class LoginFailDetectWarning extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning>{
        // 定义属性，登录失败次数上限
        private Integer maxLoginFails;

        public LoginFailDetectWarning(Integer maxLoginFails) {
            this.maxLoginFails = maxLoginFails;
        }

        // 定义状态，保存登录失败事件
        ListState<LoginEvent> loginEventListState;
        // 定义状态，保存定时器时间戳，用于定时器清除操作
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 对定义的状态进行初始化
            loginEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail-list", LoginEvent.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void processElement(LoginEvent loginEvent, KeyedProcessFunction<Long, LoginEvent, LoginFailWarning>.Context context, Collector<LoginFailWarning> collector) throws Exception {
            // 判断当前登录事件类型
            if ("fail".equals(loginEvent.getLoginState())){
                // 如果是失败事件，添加到表状态中
                loginEventListState.add(loginEvent);
                // 如果没有定时器，注册一个2秒之后的定时器
                if (null == timerTsState.value()){
                    long ts = (loginEvent.getTimestamp() + 2) * 1000L;
                    context.timerService().registerEventTimeTimer(ts);
                    timerTsState.update(ts);
                }
            }else {
                // 如果定是登录成功，删除定时器，清空状态，重新开始
                if (null != timerTsState.value())
                    context.timerService().deleteEventTimeTimer(timerTsState.value());
                loginEventListState.clear();
                timerTsState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, LoginEvent, LoginFailWarning>.OnTimerContext ctx, Collector<LoginFailWarning> out) throws Exception {
            // 获取登录失败状态，获取登录失败次数
            ArrayList<LoginEvent> loginFailEvents = Lists.newArrayList(loginEventListState.get().iterator());
            int failTimes = loginFailEvents.size();

            // 定时触发的时候，如果失败次数达到失败最大次数，进行报警输出，然后清空状态
            if (failTimes >= maxLoginFails){
                out.collect(new LoginFailWarning(ctx.getCurrentKey(),
                        loginFailEvents.get(0).getTimestamp(),
                        loginFailEvents.get(failTimes -1).getTimestamp(),
                        "login fail in 2s for " + failTimes + " times"));
            }
            loginEventListState.clear();
            timerTsState.clear();
        }
    }
}
