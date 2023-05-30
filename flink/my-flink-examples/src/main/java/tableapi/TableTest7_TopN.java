package tableapi;

public class TableTest7_TopN {
    /*

    Top-N

    Top-N查询要求按列排序的N个最小或最大值。最小值集和最大值集都被认为是Top-N查询。
    如果需要在某个条件下只显示批处理/流式处理表中的N个最底部或N个最顶部的记录，则Top-N查询非常有用。此结果集可用于进一步分析。
    Flink使用OVER窗口子句和筛选条件的组合来表示Top-N查询。凭借OVER window PARTITION BY子句的强大功能，Flink还支持按组Top-N。
    例如，每个类别中实时销售额最高的前五种产品。SQL在批处理表和流表上支持Top-N查询。

    SELECT [column_list]
FROM (
   SELECT [column_list],
     ROW_NUMBER() OVER ([PARTITION BY col1[, col2...]]
       ORDER BY col1 [asc|desc][, col2 [asc|desc]...]) AS rownum
   FROM table_name)
WHERE rownum <= N [AND conditions]


       目前，我们只支持ROW_NUMBER作为窗口上的函数。未来，我们将支持RANK（）和DENSE_RANK（）。
       不同列的排序方向可能不同。
       Flink需要rownum<=N才能识别此查询是Top-N查询。N表示将保留的最小或最大的N个记录。
       在where子句中可以自由添加其他条件，但其他条件只能使用AND连接与rownum<=N组合。

       注意：必须严格遵循上面的模式，否则优化器将无法转换查询。

       TopN查询是Result Updating。Flink SQL会根据顺序键对输入数据流进行排序，
       因此如果前N条记录发生了更改，则更改后的记录将作为撤回/更新记录发送到下游。
       建议使用支持更新的存储器作为Top-N查询的接收器。此外，如果前N条记录需要存储在外部存储器中，则结果表应具有与前N条查询相同的唯一Key。

       Top-N查询的唯一key是分区列和行数列的组合。Top-N查询还可以导出上行的唯一密钥。


       如上所述，rownum字段将作为唯一键的一个字段写入结果表，这可能导致大量记录被写入结果表。
       例如，当排名9的记录（比如product-1001）被更新，并且其排名被升级为1时，排名1~9的所有记录都将作为更新消息输出到结果表中。
       如果结果表接收的数据太多，它将成为SQL作业的瓶颈。

       优化方法是省略Top-N查询的外部SELECT子句中的rownum字段。这是合理的，因为前N个记录的数量通常不多，因此消费者可以自己快速地对记录进行排序。
       在没有rownum字段的情况下，在上面的例子中，只需要将更改后的记录（product-1001）发送到下游，这可以减少结果表的IO。

       注意：在流模式下，为了将上述查询输出到外部存储器并获得正确的结果，外部存储器必须具有与Top-N查询相同的唯一key。
       在上面的示例查询中，如果product_id是查询的唯一key，那么外部表也应该将product_id作为唯一key。


       Window Top-N
       Window Top-N是一个特殊的Top-N，它为每个窗口和其他分区键返回N个最小或最大的值。
       对于流式查询，与连续表上的常规Top-N不同，窗口Top-N不发出中间结果，而只发出最终结果，即窗口末尾的前N个记录总数。
       此外，窗口Top-N在不再需要时清除所有中间状态。因此，如果用户不需要按记录更新结果，则窗口Top-N查询具有更好的性能。

       通常，Window Top-N直接与Windowing TVF一起使用。
       此外，Window Top-N还可以与其他基于Windowing TVF的操作一起使用，如Window Aggregation、Window TopN和Window Join。

       SELECT [column_list]
        FROM (
           SELECT [column_list],
             ROW_NUMBER() OVER (PARTITION BY window_start, window_end [, col_key1...]
               ORDER BY col1 [asc|desc][, col2 [asc|desc]...]) AS rownum
           FROM table_name) -- relation applied windowing TVF
        WHERE rownum <= N [AND conditions]


        限制：
        目前，Flink只支持Window Top-N 紧跟 Tumble窗口、Hop窗口和 Cumulate 窗口的Windowing TVF的后面。
        在不久的将来将支持使用 Session 窗口的Windowing TVF。


     */
}
