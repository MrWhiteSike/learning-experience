package watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * 自定义周期性watermark生成器；
 *
 * 该 watermark 生成器可以覆盖的场景是：数据源在一定程度上乱序。
 * 即某个最新到达的时间戳为 t 的元素将在最早到达的时间戳为 t 的元素之后最多 n 毫秒到达。
 */
public class BoundedOutOfOrdernessGenerator implements WatermarkGenerator<Object> {

    private final long maxOutOfOrderness = 3500; // 3.5 秒
    private long currentMaxTimestamp;

    @Override
    public void onEvent(Object event, long eventTimestamp, WatermarkOutput output) {
        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // 发出的watermark = 当前最大时间戳 - 最大乱序时间
        output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness -1));
    }
}
