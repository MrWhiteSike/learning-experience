package tableapi;

public class TableTest8_Deduplication {
    /*

    Deduplication

    删除重复数据，只保留第一行或最后一行。
    在某些情况下，上游ETL作业不是一次端到端的；在故障转移的情况下，这可能会导致接收器中的记录重复。
    但是，重复的记录会影响下游分析作业（例如SUM、COUNT）的正确性，因此在进一步分析之前需要进行去重。

    Flink使用ROW_NUMBER（）来删除重复项，就像Top-N查询一样。
    理论上，重复数据消除是Top-N的一种特殊情况，其中N是一，按处理时间或事件时间排序。

    SELECT [column_list]
    FROM (
       SELECT [column_list],
         ROW_NUMBER() OVER ([PARTITION BY col1[, col2...]]
           ORDER BY time_attr [asc|desc]) AS rownum
       FROM table_name)
    WHERE rownum = 1


    为每一行指定一个唯一的连续编号，从一开始。
    指定排序列，它必须是时间属性。目前Flink支持处理时间属性和事件时间属性。按ASC排序表示保留第一行，按DESC排序表示保留最后一行。
    Flink需要rownum=1才能识别此查询是去重。

    注意：必须严格遵循上面的模式，否则优化器将无法转换查询。



    Window Deduplication

    窗口去重是一种特殊的去重，它删除在一组列上重复的行，为每个窗口和分区键保留第一个或最后一个行。

    对于流式查询，与连续表上的常规去重不同，窗口去重不会发出中间结果，而只会在窗口结束时发出最终结果。
    此外，当不再需要时，窗口重复数据消除会清除所有中间状态。因此，如果用户不需要按记录更新结果，则窗口重复数据删除查询具有更好的性能。
    通常，窗口去重直接与窗口TVF一起使用。此外，窗口去重可以与基于窗口TVF的其他操作一起使用，如窗口聚合、窗口TopN和窗口联接。

    窗口去重可以用与常规去重相同的语法进行定义，有关详细信息，请参阅重复数据删除文档。
    除此之外，Window Deduplication要求PARTITION BY子句包含关系的Window_start和Window_end列。否则，优化器将无法转换查询。
    Flink使用ROW_NUMBER（）来删除重复项，就像Window Top-N查询一样。
    理论上，Window Deduplication是Window Top-N的一种特殊情况，其中N是一，按处理时间或事件时间排序。

    SELECT [column_list]
    FROM (
       SELECT [column_list],
         ROW_NUMBER() OVER (PARTITION BY window_start, window_end [, col_key1...]
           ORDER BY time_attr [asc|desc]) AS rownum
       FROM table_name) -- relation applied windowing TVF
    WHERE (rownum = 1 | rownum <=1 | rownum < 2) [AND conditions]

    指定包含window_start、window_end和其他分区键的分区列
    指定排序列，它必须是时间属性。目前Flink支持处理时间属性和事件时间属性。按ASC排序表示保留第一行，按DESC排序表示保留最后一行。
    优化器需要rownum=1|rownum<=1|rownum<2才能识别可以转换为窗口重复数据删除的查询。

    注意：必须严格遵循上面的模式，否则优化器不会将查询转换为Window Deduplication。


    限制：
     1. 窗口去重紧跟的窗口TVF的限制：目前，如果紧跟着窗口TVF之后执行窗口去重，则窗口TVF必须使用TUMBLE窗口、HOP窗口或CUMULATE窗口，
     而不是Session 窗口。Session 窗口将在不久的将来得到支持。
     2. 排序字段的时间属性限制：当前，窗口重复数据删除要求顺序键必须是事件时间属性，而不是处理时间属性。不久的将来将支持按处理时间排序。


     */
}
