package com.bsk.app.function;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;
// 将抽象方法 提取到接口中
public interface DimAsyncJoinFunction<T> {
    void join(T input, JSONObject dimInfo) throws ParseException;
    String getKey(T input);
}
