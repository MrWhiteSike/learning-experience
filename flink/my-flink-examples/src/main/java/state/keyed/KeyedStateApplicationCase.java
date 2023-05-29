package state.keyed;

import entity.SensorReading;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 假设做一个温度报警器，如果一个传感器前后温度差超过10度就报警。
 * 这里使用键控状态 Keyed State + flatmap 来实现
 */
public class KeyedStateApplicationCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        SingleOutputStreamOperator<Tuple3<String, Double, Double>> resultStream = dataStream
                .keyBy(SensorReading::getId)
                .flatMap(new MyAppCaseMapper(10.0));

        resultStream.print();

        env.execute("application case");
    }
}
