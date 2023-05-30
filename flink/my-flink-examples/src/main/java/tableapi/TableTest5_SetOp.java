package tableapi;

public class TableTest5_SetOp {
    public static void main(String[] args) {
        /*

         UNION ：求全集
         UNION和UNION ALL返回在任一表中找到的行
         UNION 去重， UNION ALL 不去重


         INTERSECT：求交集
         INTERSECT和INTERSECT ALL返回在这两个表中都有的行。
         INTERSECT：去重
         INTERSECT ALL：不去重


         EXCEPT：求差集
         EXCEPT和EXCEPT ALL返回在一个表中不包含另一个表中的行。
         EXCEPT：去重
         EXCEPT ALL：不去重


         IN
         如果给定的表子查询中存在表达式，则返回true。子查询表必须由一列组成。此列必须与表达式具有相同的数据类型。
         优化器将IN条件重写为联接和组操作。对于流式查询，计算查询结果所需的状态可能会根据不同输入行的数量而无限增长。
         您可以为查询配置提供适当的生存时间（TTL），以防止状态大小过大。请注意，这可能会影响查询结果的正确性。


         EXISTS
         如果子查询至少返回一行，则返回true。只有在联接和组操作中可以重写操作时才支持。
         优化器将EXISTS操作重写为联接和组操作。对于流式查询，计算查询结果所需的状态可能会根据不同输入行的数量而无限增长。
         您可以为查询配置提供适当的生存时间（TTL），以防止状态大小过大。请注意，这可能会影响查询结果的正确性。
         有关详细信息，请参阅查询配置。







         */
    }
}
