package processfunction;

import entity.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class KeyedProcessFunctionSideOutputCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("low-temp") {
        };

        SingleOutputStreamOperator<SensorReading> process = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                // 判断温度，大于30度，高温流输出到主流；小于30度，低温流输出到侧输出流
                if (value.getTemperature() > 30) {
                    out.collect(value);
                } else {
                    ctx.output(outputTag, value);
                }
            }
        });

        process.print("high-temp");
        process.getSideOutput(outputTag).print("low-temp");

        env.execute("side output");
    }
}
