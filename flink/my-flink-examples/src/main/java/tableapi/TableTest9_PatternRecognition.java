package tableapi;

public class TableTest9_PatternRecognition {
    public static void main(String[] args) {
        /*
        Pattern Recognition
        模式识别

        搜索一组事件模式是一种常见的用例，尤其是在数据流的情况下。Flink附带了一个复杂的事件处理（CEP）库，该库允许在事件流中进行模式检测。
        此外，Flink的SQL API提供了一种表达查询的关系方式，其中包含大量内置函数和基于规则的优化，可以现成使用。

        它允许Flink使用MATCH_RECOGNIZE子句合并CEP和SQL API，以便在SQL中处理复杂事件。

        MATCH_RECOGNIZE子句启用以下任务：
        对partition BY和order BY子句使用的数据进行逻辑分区和排序。
        使用PATTERN子句定义要查找的行的模式。这些模式使用的语法与正则表达式的语法类似。
        行模式变量的逻辑组件在DEFINE子句中指定。
        在measures子句中定义度量值，度量值是SQL查询其他部分中可用的表达式。

        基本模式识别的语法：
        SELECT T.aid, T.bid, T.cid
        FROM MyTable
            MATCH_RECOGNIZE (
              PARTITION BY userid
              ORDER BY proctime
              MEASURES
                A.id AS aid,
                B.id AS bid,
                C.id AS cid
              PATTERN (A B C)
              DEFINE
                A AS name = 'a',
                B AS name = 'b',
                C AS name = 'c'
            ) AS T

         Flink对MATCH_RECOGNIZE子句的实现是完整标准的一个子集。

         模式识别功能在内部使用ApacheFlink的CEP库。为了能够使用MATCH_RECOGNIZE子句，需要将库作为依赖项添加到Maven项目中。

            <dependency>
              <groupId>org.apache.flink</groupId>
              <artifactId>flink-cep</artifactId>
              <version>1.15.4</version>
            </dependency>
         或者，您也可以将依赖项添加到集群类路径中
         如果要在SQL客户端中使用MATCH_RECOGNIZE子句，则不必执行任何操作，因为默认情况下会包含所有依赖项。

        SQL Semantics：SQL语义
        PARTITION BY-定义表的逻辑分区；类似于GROUP BY操作。

        ORDER BY-指定传入行的排序方式；这一点非常重要，因为模式取决于订单。

        MEASURES——定义条款的输出；类似于SELECT子句。

        ONE ROW PER MATCH -输出模式，定义每个匹配应该产生多少行。

        AFTER MATCH SKIP-指定下次匹配应该从哪里开始；这也是一种控制单个事件可以属于多少不同匹配的方法。

        PATTERN-允许构建将使用类似正则表达式的语法进行搜索的模式。

        DEFINE-本节定义了模式变量必须满足的条件。

        注意：目前，MATCH_RECOGNIZE子句只能应用于追加表。此外，它还总是生成一个追加表。







         */
    }
}
