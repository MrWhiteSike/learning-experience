package watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * 自定义周期性watermark生成器；
 *
 * 该生成器生成的watermark 滞后于处理时间固定量。它假设元素会在有限延迟后到达Flink
 */
public class TimeLagWatermarkGenerator implements WatermarkGenerator<Object> {

    private final long maxTimeLag = 5000; // 5 秒

    @Override
    public void onEvent(Object event, long eventTimestamp, WatermarkOutput output) {
        // 处理时间场景下不需要实现
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(System.currentTimeMillis() - maxTimeLag));
    }
}
