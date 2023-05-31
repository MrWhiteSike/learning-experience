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


        Partitioning
        可以在分区数据中查找模式，例如单个股票行情机或特定用户的趋势。这可以使用PARTITION BY子句来表示。该子句类似于对聚合使用GROUP BY。
        强烈建议对传入数据进行分区，否则MATCH_RECOGNIZE子句将被转换为非并行运算符，以确保全局排序。


        Order of Events
        Apache Flink允许基于时间搜索模式；处理时间或事件时间。
        在事件时间的情况下，在将事件传递到内部模式状态机之前对其进行排序。因此，无论行附加到表中的顺序如何，生成的输出都是正确的。相反，模式是按照每行中包含的时间指定的顺序进行评估的。
        MATCH_RECOGNIZE子句采用升序的时间属性作为ORDER BY子句的第一个参数。
        对于Ticker表的示例，类似ORDER BY rowtime ASC，price DESC的定义是有效的，但ORDER BY price，rowtime或ORDER BY rowtime DESC，price ASC不行。


        Define & Measures
        DEFINE和MEASURES关键字的含义与简单SQL查询中的WHERE和SELECT子句类似。
        MEASURES子句定义了匹配模式的输出中将包含的内容。它可以投影列并定义用于计算的表达式。生成的行数取决于输出模式设置。
        DEFINE子句指定了行必须满足的条件，以便将其分类到相应的模式变量中。如果没有为模式变量定义条件，则将使用默认条件，该条件对每一行的计算结果都为true。


        Aggregations
        聚合可以用于DEFINE和MEASURES子句。支持内置函数和自定义用户定义函数。
        聚合函数应用于映射到匹配的行的每个子集。

        聚合可以应用于表达式，但前提是它们引用了单个模式变量。因此，SUM（A.price*A.tax）是有效的，但AVG（A.price*B.tax）不是。
        注意：不支持DISTINCT聚合。


        Defining a Pattern ： 定义Pattern
        MATCH_RECOGNIZE子句允许用户使用与广泛使用的正则表达式语法有些相似的强大且富有表现力的语法来搜索事件流中的模式。
        每个模式都是由称为模式变量的基本构建块构建的，运算符（量词和其他修饰符）可以应用于这些构建块。整个模式必须用括号括起来。

        可以使用以下运算符：
        1. Concatenation 连接 - 类似（A B）的模式意味着A和B之间的邻接性是严格的。因此，在这两者之间不可能有未映射到A或B的行。
        2. Quantifiers 量词 - 修改可以映射到模式变量的行数。
            * — 0 or more rows
            + — 1 or more rows
            ? — 0 or 1 rows
            { n } — exactly n rows (n > 0)
            { n, } — n or more rows (n ≥ 0)
            { n, m } — between n and m (inclusive) rows (0 ≤ n ≤ m, 0 < m)
            { , m } — between 0 and m (inclusive) rows (m > 0)
        注意：不支持可能产生空匹配的模式。这种模式的例子有PATTERN（A*）、PATTERN（A？B*）、PATTERN（A｛0,｝B｛0,｝C*）等。


        Greedy & Reluctant Quantifiers
        每个量词可以是贪婪的（默认行为），也可以是不情愿的。贪婪的量词试图匹配尽可能多的行，而不情愿的量词则尝试匹配尽可能少的行。

        例如：
        SELECT *
        FROM Ticker
            MATCH_RECOGNIZE(
                PARTITION BY symbol
                ORDER BY rowtime
                MEASURES
                    C.price AS lastPrice
                ONE ROW PER MATCH
                AFTER MATCH SKIP PAST LAST ROW
                PATTERN (A B* C)
                DEFINE
                    A AS A.price > 10,
                    B AS B.price < 15,
                    C AS C.price > 12
            )

         symbol  tax   price          rowtime
        ======= ===== ======== =====================
         XYZ     1     10       2018-09-17 10:00:02
         XYZ     2     11       2018-09-17 10:00:03
         XYZ     1     12       2018-09-17 10:00:04
         XYZ     2     13       2018-09-17 10:00:05
         XYZ     1     14       2018-09-17 10:00:06
         XYZ     2     16       2018-09-17 10:00:07

        PATTERN (A B* C) : B* 是贪婪的
        结果：
         symbol   lastPrice
        ======== ===========
         XYZ      16


        PATTERN (A B*? C) ： B*? 是不情愿的
        结果：
         symbol   lastPrice
        ======== ===========
         XYZ      13
         XYZ      16

        模式变量B仅与具有价格12的行匹配，而不是吞下具有价格12、13和14的行。


        对模式的最后一个变量使用贪婪的量词是不可能的。因此，像（A B*）这样的模式是不允许的。
        这可以通过引入一个具有否定条件B的人工状态（例如C）来轻松解决。因此，您可以使用以下查询：
        PATTERN (A B* C)
        DEFINE
            A AS condA(),
            B AS condB(),
            C AS NOT condB()

        注意：目前不支持可选的不情愿量词（A？？或A｛0，1｝？）。


        Time constraint 时间限制
        特别是对于流式使用案例，通常要求模式在给定的时间段内完成。这允许限制Flink必须在内部维护的总体状态大小，即使在贪婪的量词的情况下也是如此。
        因此，Flink SQL支持额外的（非标准SQL）WITHIN子句，用于定义模式的时间约束。该子句可以在PATTERN子句之后定义，并采用毫秒分辨率的间隔。
        如果潜在匹配的第一个事件和最后一个事件之间的时间比给定值长，则不会将此类匹配追加到结果表中。

        例如：查询检测到价格在1小时内下降了10
        SELECT *
        FROM Ticker
            MATCH_RECOGNIZE(
                PARTITION BY symbol
                ORDER BY rowtime
                MEASURES
                    C.rowtime AS dropTime,
                    A.price - C.price AS dropDiff
                ONE ROW PER MATCH
                AFTER MATCH SKIP PAST LAST ROW
                PATTERN (A B* C) WITHIN INTERVAL '1' HOUR
                DEFINE
                    B AS B.price > A.price - 10,
                    C AS C.price < A.price - 10
            )

        symbol         rowtime         price    tax
        ======  ====================  ======= =======
        'ACME'  '01-Apr-11 10:00:00'   20      1
        'ACME'  '01-Apr-11 10:20:00'   17      2
        'ACME'  '01-Apr-11 10:40:00'   18      1
        'ACME'  '01-Apr-11 11:00:00'   11      3
        'ACME'  '01-Apr-11 11:20:00'   14      2
        'ACME'  '01-Apr-11 11:40:00'   9       1
        'ACME'  '01-Apr-11 12:00:00'   15      1
        'ACME'  '01-Apr-11 12:20:00'   14      2
        'ACME'  '01-Apr-11 12:40:00'   24      2
        'ACME'  '01-Apr-11 13:00:00'   1       2
        'ACME'  '01-Apr-11 13:20:00'   19      1

        结果：
        symbol         dropTime         dropDiff
        ======  ====================  =============
        'ACME'  '01-Apr-11 13:00:00'      14

        由此产生的行表示价格从15（11年4月1日12:00:00）下降到1（11月1日13:00:00）。dropDiff列包含价格差异。
        请注意，即使价格也下跌了更高的值，例如下跌了11（在11年4月1日10:00:00至11月1日11:40:00之间），这两个事件之间的时间差也大于1小时。
        因此，他们不会产生匹配。

        注意：
        通常鼓励使用WITHINE子句，因为它有助于Flink进行有效的内存管理。一旦达到阈值，就可以修剪基础状态。
        但是，WITHIN子句不是SQL标准的一部分。建议的处理时间限制的方式将来可能会改变。


        Output Mode 输出模式
        输出模式描述了每个找到的匹配应该发出多少行。SQL标准描述了两种模式：
            ALL ROWS PER MATCH
            ONE ROW PER MATCH

        目前，唯一支持的输出模式是“每匹配一行”，它将始终为每个找到的匹配生成一个输出匹配行。
        输出行的模式将是[partitioning columns]+[measure columns]按特定顺序串联而成。


        Pattern Navigation 模式导航
        DEFINE和MEASURES子句允许在（可能）与模式匹配的行列表中进行导航。
        本节讨论用于声明条件或生成输出结果的导航。

        Pattern Variable Referencing 模式变量引用
        模式变量引用允许引用DEFINE或MEASURES子句中映射到特定模式变量的一组行
        例如，表达式A.price描述了到目前为止映射到A的一组行加上当前行（如果我们试图将当前行与A匹配）。
        如果DEFINE/MMEASURES子句中的表达式需要一行（例如A.price或A.price>10），它将选择属于相应集的最后一个值。
        如果没有指定模式变量（例如SUM（price）），则表达式将引用默认模式变量*，该变量将引用模式中的所有变量。
        换句话说，它创建了一个列表，其中包含迄今为止映射到任何变量的所有行以及当前行。



        Logical Offsets
        逻辑偏移允许在映射到特定模式变量的事件中进行导航。这可以用两个相应的函数来表示：
        LAST(variable.field, n) ： 返回映射到变量最后第n个元素的事件中的字段值。计数从映射的最后一个元素开始。
        FIRST(variable.field, n)：返回映射到变量第n个元素的事件中的字段值。计数从映射的第一个元素开始。

        注意：在first/LAST函数的第一个参数中也可以使用多个模式变量引用。这样，就可以编写一个访问多列的表达式。
        但是，所有这些必须使用相同的模式变量。换句话说，LAST/FIRST函数的值必须在一行中计算。
        因此，可以使用LAST（A.price*A.tax），但不允许使用类似LAST（A.price*B.tax）的表达式。


        After Match Strategy
        AFTER MATCH SKIP子句指定在找到完整匹配后从何处开始新的匹配过程。
        有四种不同的策略：
        1. SKIP PAST LAST ROW - 在当前匹配的最后一行之后的下一行恢复模式匹配。
        2. SKIP TO NEXT ROW - 继续搜索从匹配的起始行之后的下一行开始新的匹配。
        3. SKIP TO LAST variable - 在映射到指定模式变量的最后一行恢复模式匹配。
        4. SKIP TO FIRST variable - 在映射到指定模式变量的第一行恢复模式匹配。

        这也是一种指定单个事件可以属于多少个匹配的方法。例如，使用“SKIP PAST LAST ROW”策略，每个事件最多可以属于一个匹配。

        例子：
        输入数据
         symbol   tax   price         rowtime
        ======== ===== ======= =====================
         XYZ      1     7       2018-09-17 10:00:01
         XYZ      2     9       2018-09-17 10:00:02
         XYZ      1     10      2018-09-17 10:00:03
         XYZ      2     5       2018-09-17 10:00:04
         XYZ      2     10      2018-09-17 10:00:05
         XYZ      2     7       2018-09-17 10:00:06
         XYZ      2     14      2018-09-17 10:00:07

        SELECT *
        FROM Ticker
            MATCH_RECOGNIZE(
                PARTITION BY symbol
                ORDER BY rowtime
                MEASURES
                    SUM(A.price) AS sumPrice,
                    FIRST(rowtime) AS startTime,
                    LAST(rowtime) AS endTime
                ONE ROW PER MATCH
                [AFTER MATCH STRATEGY]
                PATTERN (A+ C)
                DEFINE
                    A AS SUM(A.price) < 30
            )

         AFTER MATCH SKIP PAST LAST ROW
         symbol   sumPrice        startTime              endTime
        ======== ========== ===================== =====================
         XYZ      26         2018-09-17 10:00:01   2018-09-17 10:00:04
         XYZ      17         2018-09-17 10:00:05   2018-09-17 10:00:07

        第一个结果匹配的行： #1, #2, #3, #4.
        第二个结果匹配的行： #5, #6, #7

        AFTER MATCH SKIP TO NEXT ROW
         symbol   sumPrice        startTime              endTime
        ======== ========== ===================== =====================
         XYZ      26         2018-09-17 10:00:01   2018-09-17 10:00:04
         XYZ      24         2018-09-17 10:00:02   2018-09-17 10:00:05
         XYZ      25         2018-09-17 10:00:03   2018-09-17 10:00:06
         XYZ      22         2018-09-17 10:00:04   2018-09-17 10:00:07
         XYZ      17         2018-09-17 10:00:05   2018-09-17 10:00:07

        第一个结果匹配的行： #1, #2, #3, #4.
        相较于上一个策略，从第二行重新开始匹配。因此，第二个结果匹配的行： #2, #3, #4, #5
        第三个结果匹配的行： #3, #4, #5, #6.
        第四个结果匹配的行： #4, #5, #6, #7.
        第五个结果匹配的行： #5, #6, #7


        AFTER MATCH SKIP TO LAST A
         symbol   sumPrice        startTime              endTime
        ======== ========== ===================== =====================
         XYZ      26         2018-09-17 10:00:01   2018-09-17 10:00:04
         XYZ      25         2018-09-17 10:00:03   2018-09-17 10:00:06
         XYZ      17         2018-09-17 10:00:05   2018-09-17 10:00:07

        第一个结果匹配的行： #1, #2, #3, #4.
        相较于上一个策略，从第三行重新开始匹配 (第三行：匹配A的最后一行) 。因此，第二个结果匹配的行：#3, #4, #5, #6
        和第二条逻辑相同，第三个结果匹配的行：#5, #6, #7


        AFTER MATCH SKIP TO FIRST A
        这种组合将产生运行时异常，因为人们总是试图在上一个匹配开始的地方开始新的匹配。这将产生一个无限循环，因此是被禁止的。
        必须记住，在SKIP to FIRST/LAST变量策略的情况下，可能没有映射到该变量的行（例如，对于模式A*）。
        在这种情况下，将抛出运行时异常，因为正常情况需要一个有效的行来继续匹配。


        Time attributes 时间属性
        为了在MATCH_RECOGNIZE之上应用一些后续查询，可能需要使用时间属性。要选择这些功能，有两个可用功能：
        1. MATCH_ROWTIME([rowtime_field])
            返回映射到给定模式的最后一行的时间戳。
            函数接受零或一个操作数，该操作数是具有rowtime属性的字段引用。如果没有操作数，函数将返回TIMESTAMP类型的rowtime属性。否则，返回类型将与操作数类型相同。
            生成的属性是事件时间属性，可用于后续基于时间的操作，如间隔联接和组窗口或跨窗口聚合。
        2. MATCH_PROCTIME()
            生成的属性是处理时间属性，可用于后续基于时间的操作，如间隔联接和组窗口或跨窗口聚合。




        Controlling Memory Consumption 控制内存消耗
        在编写MATCH_RECOGNIZE查询时，内存消耗是一个重要的考虑因素，因为潜在匹配的空间是以广度优先的方式构建的。
        考虑到这一点，我们必须确保模式匹配能够完成。优选地，具有映射到匹配的合理数量的行，因为它们必须存入内存中。

        例如，模式不能有一个没有上限的量词来接受每一行。这样的模式可能是这样的：
        PATTERN (A B+ C)
        DEFINE
          A as A.price > 10,
          C as C.price > 20



        限制：
        Flink对MATCH_RECOGNIZE子句的实现是一项持续的工作，SQL标准的一些特性还不受支持。
        不支持的功能包括：
            1. Pattern expressions:
                Pattern groups - 这意味着例如量词不能应用于模式的子序列。因此，（A（B C）+）不是一个有效的模式。
                Alterations - 模式如PATTERN（（A B|C D）E），这意味着在查找E行之前必须找到子序列A B或C D。
                PERMUTE operator - 它等价于它所应用的变量的所有排列，例如PATTERN（PERMUTE（A，B，C））＝ PATTERN (A B C | A C B | B A C | B C A | C A B | C B A) 。
                Anchors - ^, $, 表示分区的开始/结束，这些在流上下文中没有意义，也将不受支持。
                Exclusion - PATTERN ({- A -} B) 这意味着A将被寻找但不会参与输出。这只适用于 ALL ROWS PER MATCH 模式。
                Reluctant optional quantifier - PATTERN A?? 只支持贪婪的可选量词。
            2. ALL ROWS PER MATCH 输出模式目前不支持
            3. SUBSET - 这允许创建模式变量的逻辑组并在DEFINE和MEASURES子句中使用这些组。
            4. Physical offsets - PREV/NEXT, 它索引所有看到的事件，而不仅仅是映射到模式变量的事件
            5. 提取时间属性-目前不可能为后续基于时间的操作获取时间属性。
            6. 仅SQL支持MATCH_RECOGNIZE。表API中没有等效项
            7. 不支持distinct聚合。









         */
    }
}
