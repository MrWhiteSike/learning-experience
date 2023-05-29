package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnectUtil {
    /**
     * 获取连接
     * @param url URL地址
     * @param user 用户名
     * @param password 用户密码
     * @return 连接
     * @throws SQLException sql异常
     */
    public static Connection getConnection(String url, String user, String password){
        Connection connection = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(url, user, password);
            // 设置手动提交
            connection.setAutoCommit(false);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 提交事务
     * @param connection
     */
    public static void commit(Connection connection){
        if (connection != null){
            try {
                connection.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                close(connection);
            }
        }
    }

    /**
     * 事务回滚
     * @param connection
     */
    public static void rollback(Connection connection){
        if (connection != null){
            try {
                connection.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                close(connection);
            }
        }
    }

    /**
     * 关闭连接
     * @param connection
     */
    public static void close(Connection connection){
        if (connection != null){
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
