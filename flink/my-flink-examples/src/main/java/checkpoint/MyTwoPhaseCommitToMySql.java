package checkpoint;

import entity.SensorReading;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import util.DBConnectUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 这种写法有问题：
 * 不能正常运行
 */
public class MyTwoPhaseCommitToMySql extends TwoPhaseCommitSinkFunction<SensorReading, Connection, Void> {

    public MyTwoPhaseCommitToMySql() {
        super(TypeInformation.of(Connection.class).createSerializer(new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    /**
     * 执行数据入库操作
     * @param connection Mysql连接
     * @param value 输入数据
     * @param context 上下文
     * @throws Exception
     */
    @Override
    protected void invoke(Connection connection, SensorReading value, Context context) throws Exception {
        System.out.println("start invoke.......");
        String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        System.out.println("=====>date:" + date + " " + value.toString());
        String sql = "insert into sensor(id, timestamp , temperature) values (?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, value.getId());
        ps.setLong(2, value.getTimestamp());
        ps.setDouble(3, value.getTemperature());
        // 执行insert语句
        ps.execute();
    }

    /**
     * 获取连接，开启手动提交事务（）
     * @return
     * @throws Exception
     */
    @Override
    protected Connection beginTransaction() throws Exception {
        String url = "jdbc:mysql://localhost:3306/flink_test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true";
        Connection connection = DBConnectUtil.getConnection(url, "root", "root");
        System.out.println("start beginTransaction......." + connection);
        return connection;
    }

    /**
     * 预提交，这里预提交的逻辑在invoke方法中
     * @param transaction
     * @throws Exception
     */
    @Override
    protected void preCommit(Connection transaction) throws Exception {
        System.out.println("start preCommit......."+ transaction);
    }

    /**
     * 如果invoke执行正常则提交事务
     * @param transaction
     */
    @Override
    protected void commit(Connection transaction) {
        System.out.println("start commit......."+transaction);
        DBConnectUtil.commit(transaction);
    }

    /**
     * 如果invoke执行异常则回滚事务，下一次的checkpoint操作也不会执行
     * @param transaction
     */
    @Override
    protected void abort(Connection transaction) {
        System.out.println("start abort rollback......."+ transaction);
        DBConnectUtil.rollback(transaction);

    }
}
