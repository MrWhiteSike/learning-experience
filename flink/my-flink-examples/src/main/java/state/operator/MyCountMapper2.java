package state.operator;

import entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;

import java.util.Collections;
import java.util.List;

public class MyCountMapper2 implements MapFunction<SensorReading, Integer>, ListCheckpointed<Integer> {
    private Integer count = 0;
    @Override
    public Integer map(SensorReading value) throws Exception {
        count++;
        return count;
    }

    @Override
    public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
        return Collections.singletonList(count);
    }

    @Override
    public void restoreState(List<Integer> state) throws Exception {
        for (Integer num : state){
            count += num;
        }
    }
}
