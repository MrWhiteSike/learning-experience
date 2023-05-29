package checkpoint;

import entity.SensorReading;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;


public class MyTwoPhaseCommitToMySql_New extends TwoPhaseCommitSinkFunction<SensorReading, MyContentTransaction, Void> {

    public MyTwoPhaseCommitToMySql_New() {
        super(new KryoSerializer<>(MyContentTransaction.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    @Override
    protected void invoke(MyContentTransaction transaction, SensorReading value, Context context) throws Exception {
        transaction.store(value);
    }

    @Override
    protected MyContentTransaction beginTransaction() throws Exception {
        return new MyContentTransaction();
    }

    @Override
    protected void preCommit(MyContentTransaction transaction) throws Exception {
        System.out.println("start preCommit......."+ transaction);
    }

    @Override
    protected void commit(MyContentTransaction transaction) {
        transaction.commit();
    }

    @Override
    protected void abort(MyContentTransaction transaction) {
        transaction.rollback();
    }
}
