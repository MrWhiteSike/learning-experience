package checkpoint;

import entity.SensorReading;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MySinkToMySql extends GenericWriteAheadSink<SensorReading>{

    private Connection connection = null;
    private PreparedStatement preparedStatement = null;

    @Override
    public void open() throws Exception {
        super.open();
        connection = getConnection();
        String sql = "insert into sensor(id, timestamp , temperature) values (?,?,?)";
        if (connection != null){
            preparedStatement = this.connection.prepareStatement(sql);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null){
            connection.close();
        }
        if (preparedStatement != null){
            preparedStatement.close();
        }
    }

    public MySinkToMySql() throws Exception {
        super(new SimpleCommitter(),
                TypeExtractor.getForClass(SensorReading.class).createSerializer(new ExecutionConfig()), "job");
    }

    @Override
    protected boolean sendValues(Iterable<SensorReading> values, long checkpointId, long timestamp) throws Exception {
        for (SensorReading value : values){
            preparedStatement.setString(1, value.getId());
            preparedStatement.setLong(2, value.getTimestamp());
            preparedStatement.setDouble(3, value.getTemperature());
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
        return true;
    }

    private static Connection getConnection(){
        Connection con = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink_test", "root", "root");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return con;
    }


    private static class SimpleCommitter extends CheckpointCommitter {

        private List<Tuple2<Long, Integer>> checkpoints;

        @Override
        public void open() throws Exception {

        }

        @Override
        public void close() throws Exception {

        }

        @Override
        public void createResource() throws Exception {
            checkpoints = new ArrayList<>();
        }

        @Override
        public void commitCheckpoint(int subtaskIdx, long checkpointID) throws Exception {
            checkpoints.add(new Tuple2<>(checkpointID, subtaskIdx));
        }

        @Override
        public boolean isCheckpointCommitted(int subtaskIdx, long checkpointID) throws Exception {
            return checkpoints.contains(new Tuple2<>(checkpointID, subtaskIdx));
        }
    }
}


