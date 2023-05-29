package checkpoint;

import entity.SensorReading;
import util.DBConnectUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MyContentTransaction {
    private final List<SensorReading> storeData = new ArrayList<>();
    private transient Connection connection;

    public void store(SensorReading v){
        storeData.add(v);
    }

    public void commit() {
        String url = "jdbc:mysql://localhost:3306/flink_test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true";
        connection = DBConnectUtil.getConnection(url, "root", "root");
        String sql = "insert into sensor(id, timestamp , temperature) values (?,?,?)";
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(sql);
            for (SensorReading value:storeData){
                ps.setString(1, value.getId());
                ps.setLong(2, value.getTimestamp());
                ps.setDouble(3, value.getTemperature());
                ps.addBatch();
            }
            ps.executeBatch();
            connection.commit();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            DBConnectUtil.close(connection);
        }
    }

    public void rollback() {
        try {
            connection.rollback();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            DBConnectUtil.close(connection);
        }
    }

}
