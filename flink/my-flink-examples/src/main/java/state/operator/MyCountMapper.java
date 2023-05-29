package state.operator;

import entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;


public class MyCountMapper implements MapFunction<SensorReading, Integer>, CheckpointedFunction {

    // 定义一个本地变量，作为算子状态
    private Integer count = 0;
    private ListState<Integer> listState;

    // Map 的核心处理逻辑: 分区来一条数据，计数+1
    @Override
    public Integer map(SensorReading value) throws Exception {
        count++;
        return count;
    }

    // 重写CheckpointedFunction中的snapshotState
    // 将本地缓存snapshot保存到存储上
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // 将之前的checkpoint清理
        listState.clear();
        // 将最新数据写到状态中
        listState.add(count);
    }


    // 重写CheckpointedFunction中initializeState
    // 初始化状态
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 注册 ListStateDescriptor
        ListStateDescriptor<Integer> stateDescriptor = new ListStateDescriptor<>("state", TypeInformation.of(Integer.class));
        // 从 FunctionInitializationContext 中获取OperatorStateStore，进而获取ListState
        listState = context.getOperatorStateStore().getListState(stateDescriptor);

        // 如果是作业重启，读取存储中的状态数据并填充到本地缓存中
        if (context.isRestored()){
            for (Integer i : listState.get()){
                count += i;
            }
        }
    }

}
