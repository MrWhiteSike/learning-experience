package com.bsk.app.function;

import com.alibaba.fastjson.JSONObject;
import com.bsk.common.GmallConfig;
import com.bsk.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * Rich function 中有声明周期函数
 *
 */

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    // 声明一个 jdbc连接 属性
    private Connection connection=null;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 在生命周期函数中创建JDBC连接
        Class.forName(GmallConfig.PHOENIX_DRIVER); // 加载驱动
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        // Phoenix自动提交：方式1 ； MySQL 设置自动提交为true； Phoenix中的自动提交为false
        connection.setAutoCommit(true);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        try {
            // 获取SQL语句: 表名 + 字段 + 数据
            String sinkTable = value.getString("sinkTable");
            JSONObject after = value.getJSONObject("after");
            String upsertSql = genUpsertSql(sinkTable, after);
            // 预编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);

            // 判断如果当前数据为更新操作，则先删除Redis中的数据：为了解决数据不一致问题
            // 这个地方有一个问题：BaseDBApp 是一个进程（删redis操作）；而OrderWideApp也是一个进程（查Redis）
            // 在删除Redis操作之后与插入Phoenix新数据成功之前有一定的时间间隔，在这个时间间隔内查询Phoenix数据，往Redis中写会了老数据。就存在了 数据不一致问题？
            // 维度数据，特点是缓慢变化的，更新操作不多。
            // 解决办法：
            // 1. 不删除Redis数据，而是修改Redis数据 （这种方案更好）
            // 2. 两次删除操作，执行Phoenix插入之前和之后进行两次删除操作（部分解决，问题是中间Redis操作挂掉了，没有删除成功怎么办）
            // 3. Redis删除操作和Phoenix插入操作写在一个事务里，但是Redis是乐观锁，遇到其他操作时会立马释放锁，不太合适。
            if ("update".equals(value.getString("type"))){
                DimUtil.delRedisDimInfo(sinkTable.toUpperCase(), after.getString("id"));
            }

            // 执行插入操作: 此处可以进行批处理，也可以进行执行
            preparedStatement.executeUpdate();
            // 方式2：手动提交，这种方式可以做批量提交
//            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null){
                preparedStatement.close();
            }
        }
    }

    // value的数据格式：{"sinkTable":"dwd_order_info","database":"gmall-flink","before":{},"after":{"user_id":2022,"id":26449},"type":"insert","tableName":"order_info"}
    // 写入Phoenix中的SQL语句： upsert into db.table(id, tm_name, aa) values( '...','...','...')
    // upsert = update + insert
    private String genUpsertSql(String sinkTable, JSONObject data) {
        Set<String> keySet = data.keySet();
        Collection<Object> values = data.values();

        // Scala 中：keySet.mkString(",") ==> "id,tm_name"
        // java 中：StringUtils.join(arrays, sp)
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(keySet, ",") + ") values('" +
                StringUtils.join(values,"','") + "')";
    }
}
