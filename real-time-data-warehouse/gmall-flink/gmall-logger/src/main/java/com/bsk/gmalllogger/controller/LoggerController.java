package com.bsk.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

//@Controller
@RestController // 表示 @Controller + @ResponseBody，并且该类中的所有接口返回的都是一个普通的Java对象
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate; // 这里的报错，是idea设置的error报错级别；如果看着不舒服，代码强迫症，可以在文件--》settings--》inspects --》spring --》 springcore--》code中进行自定义设置；即使报错也不影响程序运行，

    @RequestMapping("test1")
//    @ResponseBody // 表示返回一个普通的Java对象
    public String test1(){
        System.out.printf("success");
        return "success";
    }

    @RequestMapping("test2") // 直接访问，报400；没有响应；需要添加参数
    public String test2(@RequestParam("name") String name, // 该参数必须传
                        @RequestParam(value = "age", defaultValue = "18") int age){ // 如果参数有默认值，可以不传，使用默认值
        System.out.println("success");
        return "success";
    }

    @RequestMapping("applog")
    public String getLog(@RequestParam("param") String jsonStr){
        // 测试数据流：打印数据
//        System.out.println(jsonStr); // 由于在数据落盘的logback.xml中配置了日志数据打印到控制台，需要注释掉

        // 将数据落盘：
        // 1.添加logback.xml 2.添加@slf4注解 3.通过Logger对象log调用对应方法进行日志级别操作
        // 下边的级别越来越高：
//        log.debug(jsonStr); // debug信息
        log.info(jsonStr); // 信息 ，由于在logback.xml中设置就是这个级别，需要选择这个级别进行操作
//        log.warn(jsonStr); // 警告
//        log.error(jsonStr); // 错误的
//        log.trace(jsonStr); // 致命的


        // 将数据写入kafka
        kafkaTemplate.send("ods_base_log", jsonStr);

        return "success";
    }

}
