package com.bsk.flink.beans;

/**
 * Created by baisike on 2022/3/21 11:09 下午
 */
public class ChannelPromotionCount {
    private String channel;
    private String behavior;
    private String windowEnd;
    private Long count;

    public ChannelPromotionCount() {
    }

    public ChannelPromotionCount(String channel, String behavior, String windowEnd, Long count) {
        this.channel = channel;
        this.behavior = behavior;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public String getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(String windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "ChannelPromotionCount{" +
                "channel='" + channel + '\'' +
                ", behavior='" + behavior + '\'' +
                ", windowEnd='" + windowEnd + '\'' +
                ", count=" + count +
                '}';
    }
}
