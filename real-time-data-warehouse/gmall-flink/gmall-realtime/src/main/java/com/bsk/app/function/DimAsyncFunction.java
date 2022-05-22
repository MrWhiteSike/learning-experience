package com.bsk.app.function;

import com.alibaba.fastjson.JSONObject;
import com.bsk.common.GmallConfig;
import com.bsk.utils.DimUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimAsyncJoinFunction<T> {
    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;
    private String tableName;

    public DimAsyncFunction(String tableName){
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    // 查询维度信息:
                    // 表名 如何获取？通过构造器传参 ；
                    // id 如何从input中获取，因为是泛型，如何获取呢？ 抽象方法，抽象类，创建类的实例时候在外部实现抽象方法来实现。
                    String id = getKey(input);
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, id);
                    // 补充维度信息 : dimInfo 的 数据要补充到T上，而且每张表要补充的字段都不一样，还是抽象方法 来解决
                    if (dimInfo != null){
                        join(input, dimInfo);
                    }

                    // 将数据输出
                    resultFuture.complete(Collections.singletonList(input));

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    // 生产环境中，我们将抽象方法给提取出去，放到接口中，以便提高复用性；别人要使用到这些抽象方法，可以直接实现接口就好了
//    public abstract void join(T input, JSONObject dimInfo) throws ParseException;
//    public abstract String getKey(T input);

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        // 生产环境中可以重新请求
        System.out.println("TimeOut : "  + input);
    }
}
