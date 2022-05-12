package com.bsk.app.function;

import com.alibaba.fastjson.JSONObject;
import com.bsk.common.GmallConfig;
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
            String upsertSql = genUpsertSql(value.getString("sinkTable"), value.getJSONObject("after"));
            // 预编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);
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
