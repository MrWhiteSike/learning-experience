package com.bsk.flink.api.transform;

/**
 * Created by baisike on 2022/3/14 2:10 下午
 */
public class TransformTest6_Partition {
//    broadcast 广播，向每个下游分区都分发一份
//    shuffle 随机 类似发牌
//    forward 直通
//    rebalance 轮询 不同分区之间数据传输时，默认的传输方式
//    rescale 重新平衡：分区先进行分组，然后在分区组内进行轮询
//    global 汇总数据到第一个分区，谨慎使用，因为不能并行执行
//    partitionCustom 自定义分区
}
