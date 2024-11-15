package com.bsk.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {

    static ThreadPoolExecutor threadPoolExecutor = null;

    private ThreadPoolUtil(){}

    public static ThreadPoolExecutor getThreadPool(){
        if (threadPoolExecutor == null){
            synchronized (ThreadPoolExecutor.class){
                if (threadPoolExecutor == null){
                    threadPoolExecutor = new ThreadPoolExecutor(8,
                            16,
                            1L,
                            TimeUnit.MINUTES,
                            new LinkedBlockingDeque<>());
                }
            }
        }
        return threadPoolExecutor;
    }
}
