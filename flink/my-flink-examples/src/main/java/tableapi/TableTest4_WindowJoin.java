package tableapi;

public class TableTest4_WindowJoin {
    public static void main(String[] args) {
        /*
         * Window Join
         *
         * 窗口联接将时间维度添加到联接条件本身中。在这样做的过程中，窗口连接将共享一个公共Key并且在同一窗口中的两个流的元素连接起来。
         * 对于流式查询，与连续表上的其他联接不同，窗口 join 不发出中间结果，而只在窗口末尾发出最终结果。此外，窗口连接在不需要时清除所有中间状态。
         *
         * 通常，Window Join与Windowing TVF一起使用。
         * 此外，Window Join可以在基于Windowing TVF的其他操作之后进行，如Window Aggregation、Window TopN和Window Join。
         *
         * 目前，窗口联接要求联接条件包含输入表的窗口开始相等和输入表的窗口结束相等。
         * Window Join 支持 INNER/LEFT/RIGHT/FULL OUTER/ANTI/SEMI JOIN.
         *
         *
         * SEMI：
         * IN EXISTS
         *
         * SELECT *
           FROM (
               SELECT * FROM TABLE(TUMBLE(TABLE LeftTable, DESCRIPTOR(row_time), INTERVAL '5' MINUTES))
           ) L WHERE L.num IN (
             SELECT num FROM (
               SELECT * FROM TABLE(TUMBLE(TABLE RightTable, DESCRIPTOR(row_time), INTERVAL '5' MINUTES))
             ) R WHERE L.window_start = R.window_start AND L.window_end = R.window_end);

            SELECT *
           FROM (
               SELECT * FROM TABLE(TUMBLE(TABLE LeftTable, DESCRIPTOR(row_time), INTERVAL '5' MINUTES))
           ) L WHERE EXISTS (
             SELECT * FROM (
               SELECT * FROM TABLE(TUMBLE(TABLE RightTable, DESCRIPTOR(row_time), INTERVAL '5' MINUTES))
             ) R WHERE L.num = R.num AND L.window_start = R.window_start AND L.window_end = R.window_end);
         *
         *
         * ANTI：
         * NOT IN
         * NOT EXISTS
         *
         * SELECT *
           FROM (
               SELECT * FROM TABLE(TUMBLE(TABLE LeftTable, DESCRIPTOR(row_time), INTERVAL '5' MINUTES))
           ) L WHERE L.num NOT IN (
             SELECT num FROM (
               SELECT * FROM TABLE(TUMBLE(TABLE RightTable, DESCRIPTOR(row_time), INTERVAL '5' MINUTES))
             ) R WHERE L.window_start = R.window_start AND L.window_end = R.window_end);
         *
         *  SELECT *
           FROM (
               SELECT * FROM TABLE(TUMBLE(TABLE LeftTable, DESCRIPTOR(row_time), INTERVAL '5' MINUTES))
           ) L WHERE NOT EXISTS (
             SELECT * FROM (
               SELECT * FROM TABLE(TUMBLE(TABLE RightTable, DESCRIPTOR(row_time), INTERVAL '5' MINUTES))
             ) R WHERE L.num = R.num AND L.window_start = R.window_start AND L.window_end = R.window_end);
         *
         *
         * 限制：
         * 1. Join 子句上的限制
         * 目前，窗口联接要求联接条件包含输入表的窗口开始相等和输入表的窗结束相等。
         * 将来，如果窗口TVF是TUMBLE或HOP，我们还可以简化join-on子句，使其仅包括窗口开始相等。
         *
         * 2. 输入的 windowing TVFs 上的限制
         * 目前，窗口TVF必须是左右输入相同。这可以在将来扩展，例如，滚动窗口连接具有相同窗口大小的滑动窗口。
         *
         * 3. 紧跟在 Windowing TVF 之后的Window Join上的限制
         * 目前，如果Window Join在Windowing TVF之后，则Windowing TVF.必须使用滚动窗口、滑动窗口或累积窗口，而不是会话窗口。
         *
         */
    }
}
