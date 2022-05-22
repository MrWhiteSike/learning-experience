package com.bsk.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Jdbc 通用工具类
 */
public class JdbcUtil {
    /**
     * 通用的数据库查询方法
     * @param connection  数据库连接
     * @param querySql 查询SQL语句
     * @param clz T类型的Class
     * @param underScoreToCamel 判断是否 下划线转驼峰命名（MySQL 字段是下划线，javabean是驼峰；需要进行转换）
     * @param <T> 数据类型
     * @return 返回数据list
     */
    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz, boolean underScoreToCamel) throws SQLException, IllegalAccessException, InstantiationException, InvocationTargetException {
        // 创建集合用于存放查询结果数据
        ArrayList<T> resultList = new ArrayList<>();

        // 预编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);
        // 执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        // 解析resultSet
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()){
            // 创建泛型对象
            T t = clz.newInstance();

            // 给泛型对象赋值
            for (int i = 1; i < columnCount+1; i++) { // 所有的JDBC 获取对象的下标都是从 1 开始的
                // 获取列名
                String columnName = metaData.getColumnName(i);

                // 判断是否需要转换为驼峰命名：使用的是 guava 包中的CaseFormat工具类
                if (underScoreToCamel){
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }

                // 获取列值
                Object value = resultSet.getObject(i);

                // 给泛型对象赋值，使用commons-beanutils包中的BeanUtils工具类
                BeanUtils.setProperty(t, columnName, value);
            }

            // 将该对象添加至集合
            resultList.add(t);
        }

        // 关闭
        preparedStatement.close();
        resultSet.close();

        // 返回结果
        return resultList;
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IllegalAccessException, InvocationTargetException, InstantiationException {
//        System.out.println(CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, "aa_bb"));
        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://hadoop1:3306/gmall-flink", "root", "123456");
        List<JSONObject> queryList = JdbcUtil.queryList(connection, "select * from user_info limit 10", JSONObject.class, false);
        for (JSONObject json:queryList) {
            System.out.println(json);
        }
        connection.close();
    }
}
