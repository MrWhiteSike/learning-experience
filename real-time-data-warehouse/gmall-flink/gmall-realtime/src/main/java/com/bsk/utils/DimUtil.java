package com.bsk.utils;

import com.alibaba.fastjson.JSONObject;
import com.bsk.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * 再次封装一下，业务相关的工具类，更方便使用
 */
public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws InvocationTargetException, SQLException, InstantiationException, IllegalAccessException {

        // 查询Phoenix之前先查询Redis
        Jedis jedis = RedisUtil.getJedis();
        /**
         * 1.存什么数据
         *  JSON string
         * 2.使用什么类型
         *  string
         * 3.Redis key
         * string : tableName + id
         * set : tableName 这个获取数据不方便，
         * Hash: tableName id
         *
         * 不选set原因：
         *      1.查询不方便
         *      2.设置过期时间
         *
         * 不选hash原因：
         * 1. 用户维度数据量大；Redis是扩展集群，数据热点问题，同一个key都在一个节点上；string可以打散
         * 2. 设置过期时间；我们希望每一条数据的过期时间都不一样；Hash是一张表一个过期时间，代表一张表中的数据要么都过期，要么都不过期，如果过期了，查询这张表中的数据，都会查询不到，进而走Phoenix查询，有点雪崩的意味；string是一条数据一个过期时间；
         */
        String redisKey = "DIM:"+ tableName + ":" + id;
        String dimInfoJsonStr = jedis.get(redisKey);

        if (dimInfoJsonStr != null ){
            // 重置过期时间
            jedis.expire(redisKey, 24 * 60 * 60);
            // 归还连接
            jedis.close();

            // 返回结果
            return JSONObject.parseObject(dimInfoJsonStr);
        }

        // 拼接查询语句
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName +
                " where id = '" + id + "'";
        // 查询Phoenix
        List<JSONObject> queryList = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);

        JSONObject dimInfoJson = queryList.get(0);
        // 返回结果之前，将数据写入Redis
        jedis.set(redisKey, dimInfoJson.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();

        // 返回结果
        return dimInfoJson;
    }

    // 删除数据
    public static void delRedisDimInfo(String tableName, String id){
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:"+ tableName + ":" + id;
        jedis.del(redisKey);
        jedis.close();
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        long start = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_USER_INFO", "1"));
        long end = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_USER_INFO", "1"));
        long end2 = System.currentTimeMillis();
        // 通过观察：发现查询速度比较慢，这个地方需要做优化
        // 优化1，旁路缓存模式：程序 --> Phoenix ==》 程序 --> redis缓存(有直接返回) --> Phoenix（没有，再查Phoenix，返回并写入缓存）
        // 优化2， 异步查询 实际上是 把维表的查询操作托管给单独的线程池完成，这样不会因为某个查询造成阻塞，单个并行可以连续发送多个请求，提高并发效率。这种方式特别针对涉及网络IO的操作，减少因为请求等待带来的消耗。 异步IO：前提要求，1。异步客户端（优点：效率更高） 2.如果没有异步客户端，可以使用线程池，多线程的方式去实现异步IO操作（优点：更加通用）；我们这里要连接Redis 和 Phoenix，采用第二种方式
        // 大部分情况下，异步交互可以大幅度提高流处理的吞吐量。
        /**
         * 实现数据流转换操作与数据库的异步 I/O 交互需要以下三部分：
         * 1.实现分发请求的 AsyncFunction
         * 2.获取数据库交互的结果并发送给 ResultFuture 的 回调 函数
         * 3.将异步 I/O 操作应用于 DataStream 作为 DataStream 的一次转换操作。
         */

        System.out.println(end - start);
        System.out.println(end2 - end);
        connection.close();
    }
}
