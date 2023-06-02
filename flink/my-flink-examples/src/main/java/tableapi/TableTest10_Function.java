package tableapi;

public class TableTest10_Function {
    public static void main(String[] args) {
        /*
        Functions 函数


        一、内置函数

        Flink Table API和SQL允许用户使用函数进行数据转换。

        函数分类
        Flink中的函数分类有两个维度。
        1.一个维度是系统（或内置）功能与目录功能。系统函数没有名称空间，只能用它们的名称引用。
        目录函数属于目录和数据库，因此它们具有目录和数据库命名空间，它们可以由完全/部分限定名称（Catalog.db.func或db.func）引用，也可以仅由函数名称引用。

        2. 另一个维度是临时函数与持久函数。临时函数是不稳定的，只能在会话的使用寿命内使用，它们总是由用户创建的。
        持久函数存在于会话的整个生命周期中，它们要么由系统提供，要么持久存在于目录中。

        这两个维度为Flink用户提供了4类函数：
        临时系统函数 Temporary system functions
        系统函数 System functions
        临时 Catalog 函数 Temporary catalog functions
        Catalog 函数 Catalog functions


        Referencing Functions
        用户可以通过两种方式引用Flink中的函数——精确引用函数或模糊引用函数。

        Precise Function Reference
        精确的函数引用使用户能够特定地、跨目录和跨数据库使用目录函数，例如，从mytable中选择mycatalog.mydb.myfunc（x），从mytab中选择mydb.myfoc（x）。
        仅从Flink 1.10开始支持此功能。

        Ambiguous Function Reference
        在不明确的函数引用中，用户只需在SQL查询中指定函数的名称，例如从mytable中选择myfunc（x）。


        Function Resolution Order 函数解析顺序
        只有当存在不同类型但名称相同的函数时，解析顺序才重要，例如，当有三个函数都命名为“myfunc”，但分别为临时目录、目录和系统函数时。
        如果没有函数名称冲突，函数将被解析为唯一的一个。

        Precise Function Reference
        由于系统函数没有名称空间，Flink中的精确函数引用必须指向临时目录函数或目录函数。
        解析顺序：
        1. Temporary catalog function
        2. Catalog function


        Ambiguous Function Reference
        解析顺序：
        1. Temporary system function
        2. System function
        1. Temporary catalog function 在会话的当前目录和当前数据库中
        2. Catalog function 在会话的当前目录和当前数据库中



        System (Built-in) Functions
        Flink Table API和SQL为用户提供了一组用于数据转换的内置函数。

        Scalar Functions
        标量函数以零、一个或多个值作为输入，并返回一个值作为结果。

        比较函数
        = <> > >= < <=
        is null
        is not null
        is distinct from
        is not distinct from
        between and
        not between and
        like
        not like
        similar to
        not similar to
        in (value,value...)
        not in (value,value...)
        exists(子查询)
        in (子查询)
        not in (子查询)

        逻辑函数
        or
        and
        not
        is false
        is not false
        is true
        is not true
        is unknown
        is not unknown


        算术函数
        + - * / %
        power(,)
        abs()
        sqrt()
        ln()
        log10()
        log2()
        log()
        log(,)
        exp()
        ceil()
        ceiling()
        floor()
        sin()
        sinh()
        cos()
        tan()
        tanh()
        cot()
        asin()
        acos()
        atan()
        atan2(,)
        cosh()
        degrees()
        radians()
        sign()
        round(,)
        pi()
        e()
        rand()
        rand(int)
        rand_integer(int)
        rand_integer(int,int)
        uuid()
        bin()
        hex(n),hex(s)
        truncate(n,i)


        字符串函数
        || 相加
        char_length(s)
        character_length(s)
        upper(s)
        lower(s)
        position(s1 in s2) 返回在s2中第一次出现s1的位置，从1开始，没有出现返回0；
        trim([BOTH|LEADING|TRAILING] s1 from s2) 返回从s1的首尾删除s2，默认删除首尾的空格
        ltrim(s)
        rtrim(s)
        repeat(s,n) : 将s重复n次后返回
        regexp_replace(s1,s2,s3) : 在s1中，用s3替换s2后返回
        overlay(s1 placing s2 from n1 [for n2]) : 用s2覆盖s1中从n1位置开始的字符， for n2：表示覆盖几个n2个字符
        substring(s from n1 [for n2])：截取从n1开始到结束的字符，for n2 ：截取n2个的字符
        replace(s1,s2,s3)：用s3替换在s1中所有出现的s2
        regexp_extract(s1,s2[,n]) ： s2 正则表达式 ():正则匹配的组从1开始，0 表示整个正则的匹配 ，n 组的序号
        initcap(s) : 返回新形式的STRING，每个单词的第一个字符转换为大写，其余字符转换为小写。
        concat(s1,s2,...) : 字符串拼接
        concat_ws(s1,s2,s3,...) ： 拼接s2 之后的所有字符串，之间用s1进行分割
        lpad(s1,n,s2)：
        rpad(s1,n,s2)
        from_base64(s) ：对字符串进行base64解码，为null的返回null
        to_base64(s) : 对字符串进行base64编码，为null的返回null
        ascii(s) ：返回字符串中第一个字符的ASCII码
        chr(n) : 根据数字n算 ASCII字符，大于255的，对255取模 ，为null的返回null
        decode(binary,s): 对二进制数据，根据编码规则s（ ‘US-ASCII’, ‘ISO-8859-1’, ‘UTF-8’, ‘UTF-16BE’, ‘UTF-16LE’, ‘UTF-16’），解码为字符串， 任一参数为null的返回null
        encode(s1,s2) : 将s1进行编码为二进制数据，使用s2编码规则：‘US-ASCII’, ‘ISO-8859-1’, ‘UTF-8’, ‘UTF-16BE’, ‘UTF-16LE’, ‘UTF-16’， 任一参数为null的返回null
        instr(s1, s2) ： s1中出现s2的位置，任一参数为null的返回null
        left(s,n): 返回字符串左边的n个字符，如果n为负数，返回EMPTY，任一参数为null的返回null
        right(s,n): 返回字符串右边的n个字符，如果n为负数，返回EMPTY，任一参数为null的返回null
        locate(s1,s2[,n]) : [从第n位开始，不加为1]，在s2中s1出现的位置，没有返回0，任一参数为null的返回null
        parse_url(s1,s2[,s3]): s2 支持的值：‘HOST’, ‘PATH’, ‘QUERY’, ‘REF’, ‘PROTOCOL’, ‘AUTHORITY’, ‘FILE’, and ‘USERINFO’ ，任一参数为null的返回null
            QUERY时：可以使用s3，来获取参数s3对于的参数值
        regexp(s1,s2): s2 正则表达式，匹配s1中任何子串，返回TRUR，否则返回FALSE，，任一参数为null的返回null
        reverse(s): 字符串反转，为null的返回null
        split_index(s1,s2,n) : 以s2分割s1，返回字符数组中的序号为n（起始序号为0）的字符串，
        str_to_map(s1[,s2,s3]): 将s1分割成键值对形式的map，s2指定键值对之间的分割 默认值为','，s3指定键值之间的分割 默认值为 ‘=’，
        substr(s[,n1[,n2]]): 返回从n1位置开始到结束（默认）的字符，n2 ：从n1开始的n2个字符



        时间函数
        DATE string : 比如 string是 'yyyy-MM-dd' ，以这种形式返回日期
        TIME string : 比如 string是 'HH:mm:ss' , 以这种形式返回时间
        TIMESTAMP string : 比如string 是 'yyyy-MM-dd HH:mm:ss[.SSS]' ,以这种形式解析sql 时间戳
        INTERVAL string range ： range 可以是：DAY, MINUTE, DAY TO HOUR, or DAY TO SECOND YEAR or YEAR TO MONTH
            比如： INTERVAL ‘10 00:00:00.004’ DAY TO SECOND, INTERVAL ‘10’ DAY, or INTERVAL ‘2-10’ YEAR TO MONTH
        LOCALTIME ： TIME(0)
        LOCALTIMESTAMP ： TIMESTAMP(3)
        CURRENT_TIME ： LOCALTIME
        CURRENT_DATE ：
        CURRENT_TIMESTAMP ： TIMESTAMP_LTZ(3)
        NOW() ： TIMESTAMP_LTZ(3)
        CURRENT_ROW_TIMESTAMP()
        EXTRACT(timeinteravlunit FROM temporal) ： 比如 EXTRACT(DAY FROM DATE ‘2006-06-05’) 返回 5
        YEAR(date)： 比如 YEAR(DATE ‘1994-09-27’) 返回 1994
        QUARTER(date) ： = EXTRACT(QUARTER FROM date)
        MONTH(date) ： = EXTRACT(MONTH FROM date)
        WEEK(date)：= EXTRACT(WEEK FROM date) 比如，WEEK(DATE ‘1994-09-27’) 返回39
        DAYOFYEAR(date) ： = EXTRACT(DAY FROM date)
        DAYOFMONTH(date) ： 1-31
        HOUR(timestamp)：= EXTRACT(HOUR FROM timestamp) 比如，HOUR(TIMESTAMP ‘1994-09-27 13:14:15’) returns 13
        MINUTE(timestamp)： = EXTRACT(MINUTE FROM timestamp)，比如, MINUTE(TIMESTAMP ‘1994-09-27 13:14:15’) returns 14
        SECOND(timestamp): = EXTRACT(SECOND FROM timestamp)，比如，SECOND(TIMESTAMP ‘1994-09-27 13:14:15’) returns 15
        FLOOR(timepoint TO timeintervalunit) ：比如，FLOOR(TIME ‘12:44:31’ TO MINUTE) returns 12:44:00
        CEIL(timepoint TO timeintervaluntit)：比如，CEIL(TIME ‘12:44:31’ TO MINUTE) returns 12:45:00
        (timepoint1, temporal1) OVERLAPS (timepoint2, temporal2)：时间范围重叠返回true，否则返回false
        DATE_FORMAT(timestamp, string)：时间戳格式化
        TIMESTAMPADD(timeintervalunit, interval, timepoint)
        TIMESTAMPDIFF(timepointunit, timepoint1, timepoint2)：第一个参数可选值：SECOND, MINUTE, HOUR, DAY, MONTH, or YEAR
        CONVERT_TZ(string1, string2, string3)：转换时区，从string2转到string3，
            string1 是格式‘yyyy-MM-dd HH:mm:ss’ ，string2和string3 都是时区，格式：1，缩写，“PST” 2，全名 “America/Los_Angeles” 3，自定义ID “GMT-08:00”
            比如，CONVERT_TZ(‘1970-01-01 00:00:00’, ‘UTC’, ‘America/Los_Angeles’) returns ‘1969-12-31 16:00:00’
        FROM_UNIXTIME(numeric[, string])：比如，FROM_UNIXTIME(44) returns ‘1970-01-01 00:00:44’ if in UTC time zone, but returns ‘1970-01-01 09:00:44’ if in ‘Asia/Tokyo’ time zone
        UNIX_TIMESTAMP()：在指定时区下，获取当前时间的秒数时间戳
        UNIX_TIMESTAMP(string1[, string2])：在指定时区下，转换时间字符串（默认格式：yyyy-MM-dd HH:mm:ss，还可以指定string2的格式）为时间戳
        TO_DATE(string1[, string2])：转换时间字符串（默认格式：yyyy-MM-dd，还可以指定string2的格式）为日期类型
        TO_TIMESTAMP_LTZ(numeric, precision)：转变秒数或毫秒数为TIMESTAMP_LTZ类型，precision 支持 0 或 3，0代表TO_TIMESTAMP_LTZ(epochSeconds, 0)，3代表TO_TIMESTAMP_LTZ(epochMilliseconds, 3)
        TO_TIMESTAMP(string1[, string2])：在UTC时区，转换时间字符串（默认格式：yyyy-MM-dd HH:mm:ss，还可以指定string2的格式）为时间戳
        CURRENT_WATERMARK(rowtime) : 返回给定rowtime属性的当前水印，如果管道中的当前操作没有可用的所有上游操作的公共水印，则返回NULL。
            函数的返回类型被推断为与所提供的rowtime属性的返回类型匹配，但调整后的精度为3。
            例如，如果rowtime属性为TIMESTAMP_LTZ（9），则函数将返回TIMESTAMP-LTZ（3）。
            注意，此函数可以返回NULL，您可能需要考虑这种情况。比如：
            WHERE
              CURRENT_WATERMARK(ts) IS NULL
              OR ts > CURRENT_WATERMARK(ts)



        条件函数
        CASE value WHEN value1_1 [, value1_2]* THEN RESULT1 (WHEN value2_1 [, value2_2 ]* THEN result_2)* (ELSE result_z) END
            ：当 value 被包含在(value1_1,value1_2...) 中时，返回RESULT1，...,如果有else，返回 result_z，如果没有else，返回null
        CASE WHEN condition1 THEN result1 (WHEN condition2 THEN result2)* (ELSE result_z) END
            ：当 condition1 条件满足的时候，返回result1，...,如果有else，返回 result_z，如果没有else，返回null
        NULLIF(value1, value2)：如果value1=value2，返回null；否则返回value1
        COALESCE(value1 [, value2]*)：比如，COALESCE(f0, f1,..., 'default') 返回f0，f1，... 中第一个不为null的值，如果都为null，返回default
        IF(condition, true_value, false_value)：如果condition成立，返回true_value，否则返回false_value
        IFNULL(input, null_replacement)：如果input为null，返回null_replacement，否则返回input
        is_alpha(s)：如果字符串中的所有字符都是字母，则返回true，否则返回false。
        is_decimal(s)：如果字符串可以解析为有效的数字，则返回true，否则返回false。
        is_digit(s)：如果字符串中的所有字符都是数字，则返回true，否则返回false。
        GREATEST(value1[, value2]*)：返回参数列表的最大值。如果任何参数为NULL，则返回NULL。
        least(value1[, value2]*)：返回参数列表中的最小值。如果任何参数为NULL，则返回NULL。


        类型转换函数：
        CAST(value AS type)：返回要转换为新类型的值。CAST错误引发异常并使作业失败。如果  “table.exec.legacy-cast-behaviour” 是允许的，CAST 和 TRY_CAST一样
        TRY_CAST(value AS type)：类似 Cast，但是当出现错误时，返回null而不是使作业失败
        TYPEOF(input) ：返回输入的数据类型
        TYPEOF(input, force_serializable)：返回数据类型，默认情况下，返回的字符串是一个摘要字符串，为了可读性，该字符串可能会省略某些细节。
            如果force_serializable设置为TRUE，则该字符串表示可以在目录中持久化的完整数据类型。
            请注意，尤其是匿名的内联数据类型没有可序列化的字符串表示形式。在这种情况下，返回NULL。


        收集函数：
        CARDINALITY(array)：数组个数
        array '[' INT ']'：返回数组中第n个位置的元素，序号从1开始。
        ELEMENT(array)：返回数组的唯一元素（其基数应为1）；如果数组为空，则返回NULL。如果数组有多个元素，则引发异常。
        CARDINALITY(map)：返回map中键值对的个数
        map ‘[’ value ‘]’：返回map中指定key的value


        JSON函数
        路径表达式有两种风格：宽松和严格
        当省略时，默认严格模式。严格模式旨在从模式的角度检查数据，每当数据不符合路径表达式时，就会抛出错误。
        但是，像JSON_VALUE这样的函数允许在遇到错误时定义回退行为。
        宽松模式下，它更宽容，并将错误转换为空序列。
        特殊字符$表示JSON路径中的根节点。路径可以访问属性（$.a）、数组元素（$.a[0].b），或在数组中的所有元素上进行分支（$.a[*].b）
        限制：
        并非宽松模式下的所有功能当前都得到正确支持。不规范的行为不能得到保证。

        IS JSON [ { VALUE | SCALAR | ARRAY | OBJECT } ]：判断是否是有效的JSON
        JSON_EXISTS(jsonValue, path [ { TRUE | FALSE | UNKNOWN | ERROR } ON ERROR ])：
            确定JSON字符串是否满足给定的路径搜索条件。如果忽略了错误行为，则默认情况返回FALSE。
        JSON_STRING(value)：将value序列化为JSON字符串，如果value为空，则返回NULL。
        JSON_VALUE(jsonValue, path [RETURNING <dataType>] [ { NULL | ERROR | DEFAULT <defaultExpr> } ON EMPTY ] [ { NULL | ERROR | DEFAULT <defaultExpr> } ON ERROR ])
            此方法在JSON字符串中搜索给定的路径表达式，如果该路径上的值是标量，则返回该值。无法返回非标量值。默认情况下，该值返回为STRING。
            使用returningType可以选择不同的类型，支持以下类型：
                VARCHAR / STRING
                BOOLEAN
                INTEGER
                DOUBLE

            对于空路径表达式或错误，可以将行为定义为返回null、引发错误或返回定义的默认值。
            如果省略，默认值分别为NULL ON EMPTY或NULL ON ERROR。
            默认值可以是文字或表达式。如果默认值本身引发一个错误，那么它会导致ON EMPTY的错误行为，并引发ON error的错误。

        JSON_QUERY(jsonValue, path [ { WITHOUT | WITH CONDITIONAL | WITH UNCONDITIONAL } [ ARRAY ] WRAPPER ] [ { NULL | EMPTY ARRAY | EMPTY OBJECT | ERROR } ON EMPTY ] [ { NULL | EMPTY ARRAY | EMPTY OBJECT | ERROR } ON ERROR ])
            结果总是以字符串的形式返回。当前不支持RETURNING子句。
            wrappengBehavior确定是否应该将提取的值包装到数组中，以及是无条件地包装还是仅在值本身还不是数组的情况下包装。
            onEmpty和onError分别确定路径表达式为空或引发错误时的行为。默认情况下，在这两种情况下都返回null。其他选择是使用空数组、空对象或引发错误。

        JSON_OBJECT([[KEY] key VALUE value]* [ { NULL | ABSENT } ON NULL ])
            从键值对列表中构建JSON对象字符串。
            请注意，键必须是非NULL字符串文字，而值可以是任意表达式。
            此函数返回一个JSON字符串。ON NULL行为定义了如何处理NULL值。如果省略，则默认情况为NULL ON NULL。
            从另一个JSON构造函数调用（JSON_OBJECT、JSON_ARRAY）创建的值直接插入，而不是作为字符串插入。这允许构建嵌套的JSON结构。

        JSON_OBJECTAGG([KEY] key VALUE value [ { NULL | ABSENT } ON NULL ])：
            通过将键值表达式聚合到单个JSON对象中来构建JSON对象字符串。
            键表达式必须返回一个不可为null的字符串。值表达式可以是任意的，包括其他JSON函数。如果值为NULL，则ON NULL行为定义要执行的操作。
            如果忽略，则默认情况下假定为NULL ON NULL。
            注意，Key必须是唯一的。如果一个key出现多次，则会引发错误。
            OVER窗口当前不支持此函数。


        JSON_ARRAY([value]* [ { NULL | ABSENT } ON NULL ])
            从值列表中构建JSON数组字符串。
            此函数返回一个JSON字符串。这些值可以是任意表达式。ON NULL行为定义了如何处理NULL值。如果省略，默认情况下为ABSENT ON NULL。
            从另一个JSON构造函数调用（JSON_OBJECT、JSON_ARRAY）创建的元素直接插入，而不是作为字符串插入。这允许构建嵌套的JSON结构。

        JSON_ARRAYAGG(items [ { NULL | ABSENT } ON NULL ])
            通过将项聚合到数组中来构建JSON对象字符串。
            项表达式可以是任意的，包括其他JSON函数。如果值为NULL，则ON NULL行为定义要执行的操作。如果忽略，则默认情况下假定为ABSENT ON NULL。
            OVER窗口、无边界会话窗口或hop窗口当前不支持此函数。


        值构造函数
        with parenthesis (value1 [, value2]*)
            隐式构造，返回根据值列表（value1、value2、…）创建的行
            隐式行构造函数支持将任意表达式作为字段，但至少需要两个字段。显式行构造函数可以处理任意数量的字段，但目前还不能很好地支持所有类型的字段表达式。

        ARRAY ‘[’ value1 [, value2 ]* ‘]’：返回根据值列表（value1、value2、…）创建的数组。

        MAP ‘[’ value1, value2 [, value3, value4 ]* ‘]’：返回根据键值对（（value1，value2），（value3，value4），…）列表创建的Map。


        值通道函数
        tableName.compositeType.field：按名称返回Flink复合类型（例如Tuple、POJO）中字段的值
        tableName.compositeType.*：返回Flink复合类型（例如，Tuple、POJO）的扁平表示，该复合类型将其每个直接子类型转换为单独的字段。
            在大多数情况下，扁平表示的字段的名称与原始字段类似，但使用美元分隔符（例如，mypojo$mytuple$f0）


        组函数
        GROUP_ID() ： 返回一个整数，该整数唯一标识分组键的组合。
        GROUPING(expression1 [, expression2]* ) GROUPING_ID(expression1 [, expression2]* )
            返回给定分组表达式的位向量。


        Hash函数
        MD5(string)：以32位十六进制数字的字符串形式返回字符串的MD5哈希；如果字符串为NULL，则返回NULL。
        SHA1(string)：以40个十六进制数字的字符串形式返回字符串的SHA-1哈希；如果字符串为NULL，则返回NULL。
        SHA224(string)：以56个十六进制数字的字符串形式返回字符串的SHA-224哈希；如果字符串为NULL，则返回NULL。
        SHA256(string)：以64位十六进制数字的字符串形式返回字符串的SHA-256哈希；如果字符串为NULL，则返回NULL。
        SHA384(string)：以96位十六进制数字的字符串形式返回字符串的SHA-384哈希；如果字符串为NULL，则返回NULL。
        SHA512(string)：以128位十六进制数字的字符串形式返回字符串的SHA-512哈希；如果字符串为NULL，则返回NULL。
        SHA2(string, hashLength)：使用SHA-2哈希函数族（SHA-224、SHA-256、SHA-384或SHA-512）返回哈希值。
            第一个参数字符串是要散列的字符串，第二个参数hashLength是结果的比特长度（224、256、384或512）。
            如果字符串或hashLength为NULL，则返回NULL。


        聚合函数：
        聚合函数将所有行中的一个表达式作为输入，并返回单个聚合值作为结果。
        COUNT([ ALL ] expression | DISTINCT expression1 [, expression2]*)：默认情况下或使用ALL时，返回表达式不为NULL的输入行数。
            对每个值的一个唯一实例使用DISTINCT。
        COUNT(*) COUNT(1)：返回输入行数。
        AVG([ ALL | DISTINCT ] expression)：平均值
        SUM([ ALL | DISTINCT ] expression)：求和
        MAX([ ALL | DISTINCT ] expression)：最大值
        MIN([ ALL | DISTINCT ] expression )：最小值
        STDDEV_POP([ ALL | DISTINCT ] expression)：默认情况下或使用关键字ALL，返回所有输入行中表达式的总体标准偏差。对每个值的一个唯一实例使用DISTINCT。
        STDDEV_SAMP([ ALL | DISTINCT ] expression)：默认情况下，或使用关键字ALL，返回所有输入行中表达式的样本标准偏差。对每个值的一个唯一实例使用DISTINCT。
        VAR_POP([ ALL | DISTINCT ] expression)：默认情况下或使用关键字ALL，返回所有输入行中表达式的总体方差（总体标准差的平方）。对每个值的一个唯一实例使用DISTINCT。
        VAR_SAMP([ ALL | DISTINCT ] expression)：默认情况下，或使用关键字ALL，返回所有输入行中表达式的样本方差（样本标准差的平方）。对每个值的一个唯一实例使用DISTINCT。
        COLLECT([ ALL | DISTINCT ] expression)：默认情况下，或使用关键字ALL，在所有输入行中返回一个多组表达式。NULL值将被忽略。对每个值的一个唯一实例使用DISTINCT。
        VARIANCE([ ALL | DISTINCT ] expression)：同VAR_SAMP（）
        RANK()：返回一组值中某个值的秩。结果是一加上分区排序中当前行之前或等于当前行的行数。这些值将在序列中产生间隙。
        DENSE_RANK()：返回一组值中某个值的秩。结果是一加上先前指定的等级值。与函数rank不同，dense_rank不会在排名序列中产生差距。
        ROW_NUMBER()：根据窗口分区内的行顺序，为每一行指定一个唯一的序列号，从一开始。ROW_NUMBER和RANK是相似的。ROW_NUMBER按顺序对所有行进行编号（例如1、2、3、4、5）
        LEAD(expression [, offset] [, default])：返回窗口中当前行之后第二行处表达式的值。偏移量的默认值为1，默认值为NULL。
        LAG(expression [, offset] [, default])：返回窗口中当前行之前第二行的表达式值。偏移量的默认值为1，默认值为NULL。
        FIRST_VALUE(expression)：返回一组有序值中的第一个值。
        LAST_VALUE(expression)：返回一组有序值中的最后一个值。
        LISTAGG(expression [, separator])：连接字符串表达式的值，并在它们之间放置分隔符值。分隔符未添加到字符串的末尾。分隔符的默认值为“，”。



        时间间隔和点单位规范
        下表列出了时间间隔和时间点单位的说明符。
        对于表API，请使用_表示空格（例如，DAY_TO_HOUR）。
        Time Interval Unit	                Time Point Unit
        MILLENIUM (SQL-only)
        CENTURY (SQL-only)
        DECADE (SQL-only)
        YEAR	                                YEAR
        YEAR TO MONTH
        QUARTER	                                QUARTER
        MONTH	                                MONTH
        WEEK	                                WEEK
        DAY	                                    DAY
        DAY TO HOUR
        DAY TO MINUTE
        DAY TO SECOND
        HOUR	                                HOUR
        HOUR TO MINUTE
        HOUR TO SECOND
        MINUTE	                                MINUTE
        MINUTE TO SECOND
        SECOND	                                SECOND
                                                MILLISECOND
                                                MICROSECOND
        DOY (SQL-only)
        DOW (SQL-only)
        ISODOW (SQL-only)
        ISOYEAR (SQL-only)
                                                SQL_TSI_YEAR (SQL-only)
                                                SQL_TSI_QUARTER (SQL-only)
                                                SQL_TSI_MONTH (SQL-only)
                                                SQL_TSI_WEEK (SQL-only)
                                                SQL_TSI_DAY (SQL-only)
                                                SQL_TSI_HOUR (SQL-only)
                                                SQL_TSI_MINUTE (SQL-only)
                                                SQL_TSI_SECOND (SQL-only)


        列函数
        列函数用于选择或取消选择列。
        列函数仅在Table API中使用。
        withColumns(…)：选择指定的列
        withoutColumns(…)：取消选择指定的列


        columnFunction:
            withColumns(columnExprs)
            withoutColumns(columnExprs)

        columnExprs:
            columnExpr [, columnExpr]*

        columnExpr:
            columnRef | columnIndex to columnIndex | columnName to columnName

        columnRef:
            columnName(The field name that exists in the table) | columnIndex(a positive integer starting from 1)


        列函数的用法如下表所示。
        假设我们有一个有5列的表：（a:Int，b:Long，c:String，d:String，e:String）

        API	                                    Usage	                                                                                  Description
        withColumns($(*))	            select(withColumns($("*"))) = select($(“a”), $(“b”), $(“c”), $(“d”), $(“e”))	                all the columns
        withColumns(m to n)	            select(withColumns(range(2, 4))) = select($(“b”), $(“c”), $(“d”))	                            columns from m to n
        withColumns(m, n, k)	        select(withColumns(lit(1), lit(3), $(“e”))) = select($(“a”), $(“c”), $(“e”))	                columns m, n, k
        withColumns(m, n to k)	        select(withColumns(lit(1), range(3, 5))) = select($(“a”), $(“c”), $(“d”), $(“e”))	            mixing of the above two representation
        withoutColumns(m to n)	        select(withoutColumns(range(2, 4))) = select($(“a”), $(“e”))	                                deselect columns from m to n
        withoutColumns(m, n, k)	        select(withoutColumns(lit(1), lit(3), lit(5))) = select($(“b”), $(“d”))	                        deselect columns m, n, k
        withoutColumns(m, n to k)	    select(withoutColumns(lit(1), range(3, 5))) = select($(“b”))	                                mixing of the above two representation


        列函数可以用于所有需要列字段的地方，例如select、groupBy、orderBy、UDF等。


        二、用户自定义函数
        用户定义函数（UDF）是调用频繁使用的逻辑或自定义逻辑的扩展点，这些逻辑不能在查询中以其他方式表达。
        用户定义的函数可以用JVM语言（如Java或Scala）或Python实现。实现者可以在UDF中使用任意的第三方库。
        本页将重点介绍基于JVM的语言，请参阅PyFlink文档以了解有关用Python编写通用和矢量化UDF的详细信息。

        目前，Flink区分以下几种函数：
        * 标量函数将标量值映射到新的标量值。
        * 表函数将标量值映射到新行。
        * 聚合函数将多行的标量值映射到新的标量值。
        * 表聚合函数将多行的标量值映射到新行。
        * 异步表函数是用于执行查找的表源的特殊函数。

        下面的示例演示了如何创建一个简单的标量函数，以及如何在表API和SQL中调用该函数。
        对于SQL查询，函数必须始终以名称注册。对于表API，函数可以注册或直接内联使用。

        示例：
        import org.apache.flink.table.api.*;
        import org.apache.flink.table.functions.ScalarFunction;
        import static org.apache.flink.table.api.Expressions.*;

        // define function logic
        public static class SubstringFunction extends ScalarFunction {
          public String eval(String s, Integer begin, Integer end) {
            return s.substring(begin, end);
          }
        }

        TableEnvironment env = TableEnvironment.create(...);

        // call function "inline" without registration in Table API
        env.from("MyTable").select(call(SubstringFunction.class, $("myField"), 5, 12));

        // register function
        env.createTemporarySystemFunction("SubstringFunction", SubstringFunction.class);

        // call registered function in Table API
        env.from("MyTable").select(call("SubstringFunction", $("myField"), 5, 12));

        // call registered function in SQL
        env.sqlQuery("SELECT SubstringFunction(myField, 5, 12) FROM MyTable");




        对于交互式会话，也可以在使用或注册函数之前对其进行参数化。在这种情况下，可以使用函数实例而不是函数类作为临时函数。
        它要求参数是可序列化的，以便将函数实例传送到集群。
        示例：
        // call function "inline" without registration in Table API
        env.from("MyTable").select(call(new SubstringFunction(true), $("myField"), 5, 12));

        // register function
        env.createTemporarySystemFunction("SubstringFunction", new SubstringFunction(true));


        您可以使用star*表达式作为函数调用的一个参数，在表API中充当通配符，表中的所有列都将传递给相应位置的函数。
        示例：
        import org.apache.flink.table.api.*;
        import org.apache.flink.table.functions.ScalarFunction;
        import static org.apache.flink.table.api.Expressions.*;

        public static class MyConcatFunction extends ScalarFunction {
          public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object... fields) {
            return Arrays.stream(fields)
                .map(Object::toString)
                .collect(Collectors.joining(","));
          }
        }

        TableEnvironment env = TableEnvironment.create(...);

        // call function with $("*"), if MyTable has 3 fields (a, b, c),
        // all of them will be passed to MyConcatFunction.
        env.from("MyTable").select(call(MyConcatFunction.class, $("*")));

        // it's equal to call function with explicitly selecting all columns.
        env.from("MyTable").select(call(MyConcatFunction.class, $("a"), $("b"), $("c")));


        Implementation Guide 实施指南
        与函数的类型无关，所有用户定义的函数都遵循一些基本的实现原则。

        1. Function Class 创建函数类
        实现类必须从一个可用的基类（例如org.apache.flink.table.functions.ScalarFunction）扩展。
        类必须声明为公共的，而不是抽象的，并且应该是全局可访问的。因此，不允许使用非静态的内部类或匿名类。
        为了将用户定义的函数存储在持久目录中，类必须具有默认构造函数，并且必须在运行时可实例化。
        只有当函数不是有状态的（即仅包含瞬态和静态字段）时，才能持久化表API中的匿名函数。

        2. Evaluation Methods 重写计算方法
        基类提供了一组可以重写的方法，如open（）、close（）或isDeterministic（）。
        但是，除了那些声明的方法之外，应用于每个传入记录的主要运行时逻辑必须通过专门的计算方法来实现。
        根据函数类型的不同，诸如eval（）、accumulate（）或retract（）之类的求值方法在运行时由代码生成的运算符调用。

        这些方法必须声明为公共方法，并采用一组定义良好的参数。
        应用常规JVM方法调用语义。因此，可以：
        * 实现重载方法，例如eval（Integer）和eval（LocalDateTime），
        * 使用var参数，例如eval（Integer…），
        * 使用对象继承，例如同时采用LocalDateTime和Integer的eval（object），
        * 以及上面的组合，例如采用各种参数的eval（Object…）。

        如果您打算在Scala中实现函数，请在变量参数的情况下添加Scala.annotation.varargs注释。
        此外，建议使用装箱的基础类型（例如java.lang.Integer而不是Int）来支持NULL。

        示例：eval 方法可以重载
        import org.apache.flink.table.functions.ScalarFunction;

        // function with overloaded evaluation methods
        public static class SumFunction extends ScalarFunction {

          public Integer eval(Integer a, Integer b) {
            return a + b;
          }

          public Integer eval(String a, String b) {
            return Integer.valueOf(a) + Integer.valueOf(b);
          }

          public Integer eval(Double... d) {
            double result = 0;
            for (double value : d)
              result += value;
            return (int) result;
          }
        }


        Type Inference 类型推断
        表生态系统（类似于SQL标准）是一个强类型API。因此，函数参数和返回类型都必须映射到数据类型。

        从逻辑角度来看，规划者需要有关预期类型、精度和规模的信息。从JVM的角度来看，规划者需要有关在调用用户定义函数时如何将内部数据结构表示为JVM对象的信息。

        用于验证输入自变量和导出函数的参数和结果的数据类型的逻辑在术语类型推理下进行了总结。

        Flink的用户定义函数实现了自动类型推断提取，该提取通过反射从函数的类及其评估方法中派生数据类型。
        如果这种隐式反射提取方法不成功，则可以通过使用@DataTypeHint和@FunctionHint注释受影响的参数、类或方法来支持提取过程。


        如果需要更高级的类型推理逻辑，实现者可以在每个用户定义的函数中显式重写getTypeInference（）方法。
        但是，建议使用注释方法，因为它将自定义类型推理逻辑保持在受影响的位置附近，并返回到其余实现的默认行为。


        Automatic Type Inference 自动类型推断
        自动类型推理检查函数的类和求值方法，以派生函数的参数和结果的数据类型@DataTypeHint和@FunctionHint注释支持自动提取。
        对于可以隐式映射到数据类型的类的完整列表，

        @DataTypeHint
        在许多场景中，需要支持函数的参数和返回类型的自动内联提取
        以下示例显示了如何使用数据类型提示。

        示例：
        import org.apache.flink.table.annotation.DataTypeHint;
        import org.apache.flink.table.annotation.InputGroup;
        import org.apache.flink.table.functions.ScalarFunction;
        import org.apache.flink.types.Row;

        // function with overloaded evaluation methods
        public static class OverloadedFunction extends ScalarFunction {

          // no hint required
          public Long eval(long a, long b) {
            return a + b;
          }

          // define the precision and scale of a decimal
          public @DataTypeHint("DECIMAL(12, 3)") BigDecimal eval(double a, double b) {
            return BigDecimal.valueOf(a + b);
          }

          // define a nested data type
          @DataTypeHint("ROW<s STRING, t TIMESTAMP_LTZ(3)>")
          public Row eval(int i) {
            return Row.of(String.valueOf(i), Instant.ofEpochSecond(i));
          }

          // allow wildcard input and customly serialized output
          @DataTypeHint(value = "RAW", bridgedTo = ByteBuffer.class)
          public ByteBuffer eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
            return MyUtils.serializeToByteBuffer(o);
          }
        }



        @FunctionHint
        在某些场景中，希望一种eval方法同时处理多个不同的数据类型。此外，在某些场景中，重载的eval方法有一个通用的结果类型，应该只声明一次。
        @FunctionHint注释可以提供从参数数据类型到结果数据类型的映射。它允许注释输入、累加器和结果数据类型的整个函数类或求值方法。
        一个或多个注释可以在类的顶部声明，也可以为重载函数签名的每个求值方法单独声明。所有提示参数都是可选的。
        如果未定义参数，则使用默认的基于反射的提取。在函数类顶部定义的提示参数由所有求值方法继承。

        示例：
        import org.apache.flink.table.annotation.DataTypeHint;
        import org.apache.flink.table.annotation.FunctionHint;
        import org.apache.flink.table.functions.TableFunction;
        import org.apache.flink.types.Row;

        // function with overloaded evaluation methods
        // but globally defined output type
        @FunctionHint(output = @DataTypeHint("ROW<s STRING, i INT>"))
        public static class OverloadedFunction extends TableFunction<Row> {

          public void eval(int a, int b) {
            collect(Row.of("Sum", a + b));
          }

          // overloading of arguments is still possible
          public void eval() {
            collect(Row.of("Empty args", -1));
          }
        }

        // decouples the type inference from evaluation methods,
        // the type inference is entirely determined by the function hints
        @FunctionHint(
          input = {@DataTypeHint("INT"), @DataTypeHint("INT")},
          output = @DataTypeHint("INT")
        )
        @FunctionHint(
          input = {@DataTypeHint("BIGINT"), @DataTypeHint("BIGINT")},
          output = @DataTypeHint("BIGINT")
        )
        @FunctionHint(
          input = {},
          output = @DataTypeHint("BOOLEAN")
        )
        public static class OverloadedFunction extends TableFunction<Object> {

          // an implementer just needs to make sure that a method exists
          // that can be called by the JVM
          public void eval(Object... o) {
            if (o.length == 0) {
              collect(false);
            }
            collect(o[0]);
          }
        }


        Custom Type Inference 自定义类型推断
        对于大多数场景，@DataTypeHint和@FunctionHint应该足以为用户定义的函数建模。
        但是，通过重写getTypeInference（）中定义的自动类型推理，实现者可以创建行为类似于内置系统函数的任意函数。
        下面用Java实现的示例说明了自定义类型推理逻辑的潜力。它使用字符串文字参数来确定函数的结果类型。该
        函数接受两个字符串参数：第一个参数表示要解析的字符串，第二个参数表示目标类型。


        import org.apache.flink.table.api.DataTypes;
        import org.apache.flink.table.catalog.DataTypeFactory;
        import org.apache.flink.table.functions.ScalarFunction;
        import org.apache.flink.table.types.inference.TypeInference;
        import org.apache.flink.types.Row;

        public static class LiteralFunction extends ScalarFunction {
          public Object eval(String s, String type) {
            switch (type) {
              case "INT":
                return Integer.valueOf(s);
              case "DOUBLE":
                return Double.valueOf(s);
              case "STRING":
              default:
                return s;
            }
          }

          // the automatic, reflection-based type inference is disabled and
          // replaced by the following logic
          @Override
          public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
              // specify typed arguments
              // parameters will be casted implicitly to those types if necessary
              .typedArguments(DataTypes.STRING(), DataTypes.STRING())
              // specify a strategy for the result data type of the function
              .outputTypeStrategy(callContext -> {
                if (!callContext.isArgumentLiteral(1) || callContext.isArgumentNull(1)) {
                  throw callContext.newValidationError("Literal expected for second argument.");
                }
                // return a data type based on a literal
                final String literal = callContext.getArgumentValue(1, String.class).orElse("STRING");
                switch (literal) {
                  case "INT":
                    return Optional.of(DataTypes.INT().notNull());
                  case "DOUBLE":
                    return Optional.of(DataTypes.DOUBLE().notNull());
                  case "STRING":
                  default:
                    return Optional.of(DataTypes.STRING());
                }
              })
              .build();
          }
        }


        决定
        每个用户定义的函数类都可以通过重写isDeterministic（）方法来声明是否产生确定性结果。
        如果该函数不是纯函数（如random（）、date（）或now（）），则该方法必须返回false。默认情况下，isDeterministic（）返回true。
        此外，isDeterministic（）方法也可能影响运行时行为。运行时实现可以在两个不同的阶段调用：
            1. During planning (i.e. pre-flight phase)
                如果使用常量表达式调用函数，或者可以从给定语句派生常量表达式，则会对函数进行常量表达式缩减的预计算，并且可能不再在集群上执行该函数。
                除非在这种情况下使用isDeterministic（）来禁用常量表达式缩减。
                例如，在计划期间执行以下对ABS的调用：SELECT ABS（-1）FROM t和SELECT ABS（field）FROM t WHERE field=-1；而SELECT ABS（字段）FROM t不是。
            2. During runtime (i.e. cluster execution)
                如果函数是用非常数表达式调用的，或者isDeterministic（）返回false。


        Runtime Integration 运行时集成
        有时，用户定义的函数可能需要获取全局运行时信息，或者在实际工作之前进行一些设置/清理工作。
        用户定义的函数提供可重写的open（）和close（）方法，并提供与DataStream API的RichFunction中的方法类似的功能。
        open（）方法在求值方法之前调用一次。最后一次调用求值方法之后的调用close（）方法。
        open（）方法提供了一个FunctionContext，其中包含有关执行用户定义函数的上下文的信息，例如度量组、分布式缓存文件或全局作业参数。

        Method	                                    Description
        getMetricGroup()	                        此并行子任务的metric组。
        getCachedFile(name)	                        分布式缓存文件的本地临时文件副本。
        getJobParameter(name, defaultValue)	        与给定键关联的全局作业参数值。
        getExternalResourceInfos(resourceName)	    返回一组与给定密钥关联的外部资源信息。


        注意：根据执行函数的上下文，并非上述所有方法都可用。例如，在常量表达式缩减过程中，添加metric是一种非运算操作。

        示例：
        import org.apache.flink.table.api.*;
        import org.apache.flink.table.functions.FunctionContext;
        import org.apache.flink.table.functions.ScalarFunction;

        public static class HashCodeFunction extends ScalarFunction {

            private int factor = 0;

            @Override
            public void open(FunctionContext context) throws Exception {
                // access the global "hashcode_factor" parameter
                // "12" would be the default value if the parameter does not exist
                factor = Integer.parseInt(context.getJobParameter("hashcode_factor", "12"));
            }

            public int eval(String s) {
                return s.hashCode() * factor;
            }
        }

        TableEnvironment env = TableEnvironment.create(...);

        // add job parameter
        env.getConfig().addJobParameter("hashcode_factor", "31");

        // register the function
        env.createTemporarySystemFunction("hashCode", HashCodeFunction.class);

        // use the function
        env.sqlQuery("SELECT myField, hashCode(myField) FROM MyTable");


        Scalar Functions 标量函数
        用户定义的标量函数将零个、一个或多个标量值映射到新的标量值。数据类型部分中列出的任何数据类型都可以用作评估方法的参数或返回类型。
        为了定义标量函数，必须扩展org.apache.flink.table.functions中的基类ScalarFunction，并实现一个或多个名为eval（…）的求值方法。

        示例：
        import org.apache.flink.table.annotation.InputGroup;
        import org.apache.flink.table.api.*;
        import org.apache.flink.table.functions.ScalarFunction;
        import static org.apache.flink.table.api.Expressions.*;

        public static class HashFunction extends ScalarFunction {

          // take any data type and return INT
          public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
            return o.hashCode();
          }
        }

        TableEnvironment env = TableEnvironment.create(...);

        // call function "inline" without registration in Table API
        env.from("MyTable").select(call(HashFunction.class, $("myField")));

        // register function
        env.createTemporarySystemFunction("HashFunction", HashFunction.class);

        // call registered function in Table API
        env.from("MyTable").select(call("HashFunction", $("myField")));

        // call registered function in SQL
        env.sqlQuery("SELECT HashFunction(myField) FROM MyTable");


        Table Functions 表函数
        与用户定义的标量函数类似，用户定义的表函数（UDTF）采用零、一或多个标量值作为输入参数。
        但是，它可以返回任意数量的行（或结构化类型）作为输出，而不是单个值。返回的记录可能由一个或多个字段组成。
        如果输出记录仅由单个字段组成，则可以省略结构化记录，并且可以发出标量值，该标量值将由运行时隐式包装成一行。

        为了定义表函数，必须扩展org.apache.flink.table.functions中的基类TableFunction，并实现一个或多个名为eval（…）的求值方法。
        与其他函数类似，输入和输出数据类型是使用反射自动提取的。这包括用于确定输出数据类型的类的通用参数T。
        与标量函数不同，求值方法本身不能有返回类型，相反，表函数提供了一个collect（T）方法，该方法可以在每个求值方法中调用，用于发出零、一个或多个记录。

        在表API中，表函数与.joinLateral（…）或.leftOuterJoinLaterial（…）一起使用。
        joinLaterale运算符（cross）将外部表（运算符左侧的表）中的每一行与表值函数（运算符右侧的表）生成的所有行连接起来。
        leftOuterJoinLateral运算符将外部表（运算符左侧的表）中的每一行与表值函数（运算符右侧的表）生成的所有行连接起来，并保留表函数返回空表的外部行。


        在SQL中，将LATERAL TABLE（＜TableFunction＞）与JOIN一起使用，或者将LEFT JOIN与ON TRUE连接条件一起使用。

        示例：
        import org.apache.flink.table.annotation.DataTypeHint;
        import org.apache.flink.table.annotation.FunctionHint;
        import org.apache.flink.table.api.*;
        import org.apache.flink.table.functions.TableFunction;
        import org.apache.flink.types.Row;
        import static org.apache.flink.table.api.Expressions.*;

        @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
        public static class SplitFunction extends TableFunction<Row> {

          public void eval(String str) {
            for (String s : str.split(" ")) {
              // use collect(...) to emit a row
              collect(Row.of(s, s.length()));
            }
          }
        }

        TableEnvironment env = TableEnvironment.create(...);

        // call function "inline" without registration in Table API
        env
          .from("MyTable")
          .joinLateral(call(SplitFunction.class, $("myField")))
          .select($("myField"), $("word"), $("length"));
        env
          .from("MyTable")
          .leftOuterJoinLateral(call(SplitFunction.class, $("myField")))
          .select($("myField"), $("word"), $("length"));

        // rename fields of the function in Table API
        env
          .from("MyTable")
          .leftOuterJoinLateral(call(SplitFunction.class, $("myField")).as("newWord", "newLength"))
          .select($("myField"), $("newWord"), $("newLength"));

        // register function
        env.createTemporarySystemFunction("SplitFunction", SplitFunction.class);

        // call registered function in Table API
        env
          .from("MyTable")
          .joinLateral(call("SplitFunction", $("myField")))
          .select($("myField"), $("word"), $("length"));
        env
          .from("MyTable")
          .leftOuterJoinLateral(call("SplitFunction", $("myField")))
          .select($("myField"), $("word"), $("length"));

        // call registered function in SQL
        env.sqlQuery(
          "SELECT myField, word, length " +
          "FROM MyTable, LATERAL TABLE(SplitFunction(myField))");
        env.sqlQuery(
          "SELECT myField, word, length " +
          "FROM MyTable " +
          "LEFT JOIN LATERAL TABLE(SplitFunction(myField)) ON TRUE");

        // rename fields of the function in SQL
        env.sqlQuery(
          "SELECT myField, newWord, newLength " +
          "FROM MyTable " +
          "LEFT JOIN LATERAL TABLE(SplitFunction(myField)) AS T(newWord, newLength) ON TRUE");

          注意：如果您打算在Scala中实现函数，请不要将表函数作为Scala对象来实现。Scala对象是单一的，会导致并发问题。


        Aggregate Functions 聚合函数
        用户定义的聚合函数（UDAGG）将多行的标量值映射到新的标量值。
        聚合函数的行为以累加器的概念为中心。累加器是一种中间数据结构，用于存储聚合值，直到计算出最终聚合结果。
        对于每一组需要聚合的行，运行时将通过调用createAccumulator（）创建一个空的累加器。
        随后，为每个输入行调用函数的accumulate（…）方法来更新累加器。
        一旦处理完所有行，就会调用函数的getValue（…）方法来计算并返回最终结果。

        在本例中，我们假设一个表包含有关饮料的数据。该表由三列（id、name和price）和5行组成。
        我们想找到表中所有饮料的最高价格，即执行max（）聚合。我们需要考虑5行中的每一行。结果是一个单一的数值。

        为了定义聚合函数，必须扩展org.apache.flink.table.functions中的基类AggregateFunction，并实现一个或多个名为accumulate（…）的求值方法。
        accumulat方法必须公开声明，而不是静态声明。Accumulate方法也可以通过实现多个名为Accumulate的方法来重载。

        默认情况下，使用反射自动提取输入、累加器和输出数据类型。这包括用于确定累加器数据类型的类的一般参数ACC和用于确定累加器数据类型地一般参数T。
        输入参数派生自一个或多个accumulate（…）方法。

        示例：
        import org.apache.flink.table.api.*;
        import org.apache.flink.table.functions.AggregateFunction;
        import static org.apache.flink.table.api.Expressions.*;

        // mutable accumulator of structured type for the aggregate function
        public static class WeightedAvgAccumulator {
          public long sum = 0;
          public int count = 0;
        }

        // function that takes (value BIGINT, weight INT), stores intermediate results in a structured
        // type of WeightedAvgAccumulator, and returns the weighted average as BIGINT
        public static class WeightedAvg extends AggregateFunction<Long, WeightedAvgAccumulator> {

          @Override
          public WeightedAvgAccumulator createAccumulator() {
            return new WeightedAvgAccumulator();
          }

          @Override
          public Long getValue(WeightedAvgAccumulator acc) {
            if (acc.count == 0) {
              return null;
            } else {
              return acc.sum / acc.count;
            }
          }

          public void accumulate(WeightedAvgAccumulator acc, Long iValue, Integer iWeight) {
            acc.sum += iValue * iWeight;
            acc.count += iWeight;
          }

          public void retract(WeightedAvgAccumulator acc, Long iValue, Integer iWeight) {
            acc.sum -= iValue * iWeight;
            acc.count -= iWeight;
          }

          public void merge(WeightedAvgAccumulator acc, Iterable<WeightedAvgAccumulator> it) {
            for (WeightedAvgAccumulator a : it) {
              acc.count += a.count;
              acc.sum += a.sum;
            }
          }

          public void resetAccumulator(WeightedAvgAccumulator acc) {
            acc.count = 0;
            acc.sum = 0L;
          }
        }

        TableEnvironment env = TableEnvironment.create(...);

        // call function "inline" without registration in Table API
        env
          .from("MyTable")
          .groupBy($("myField"))
          .select($("myField"), call(WeightedAvg.class, $("value"), $("weight")));

        // register function
        env.createTemporarySystemFunction("WeightedAvg", WeightedAvg.class);

        // call registered function in Table API
        env
          .from("MyTable")
          .groupBy($("myField"))
          .select($("myField"), call("WeightedAvg", $("value"), $("weight")));

        // call registered function in SQL
        env.sqlQuery(
          "SELECT myField, WeightedAvg(`value`, weight) FROM MyTable GROUP BY myField"
        );

        WeightedAvg类的accumulate（…）方法接受三个输入。第一个是累加器，另外两个是用户定义的输入。
        为了计算加权平均值，累加器需要存储已累积的所有数据的加权和和和计数。
        在我们的示例中，我们将类WeightedAvgAccumulator定义为累加器。
        累加器由Flink的检查点机制自动管理，并在失败时进行恢复，以确保精确一次语义。

        Mandatory and Optional Methods 强制性和可选的方法
        以下方法对于每个AggregateFunction都是强制性的：
        * createAccumulator()
        * accumulate(...)
        * getValue(...)

        此外，还有一些方法可以选择性地实现。虽然其中一些方法允许系统更高效地执行查询，但其他方法对于某些用例是强制性的。
        例如，如果聚合函数应应用于会话组窗口的上下文中，则merge（…）方法是强制性的（当观察到“连接”两个会话窗口的行时，需要连接它们的累加器）。


        AggregateFunction的以下方法是必需的，具体取决于用例：
        * retract（…） ：对于OVER窗口上的聚合是必需的。
        * merge（…）：许多 bounded 聚合以及session 窗口和Hop窗口聚合都需要。此外，这种方法也有助于优化。
                例如，两阶段聚合优化需要所有的AggregateFunction支持合并方法。

        如果聚合函数只能在OVER窗口中应用，则可以通过在getRequirements（）中返回需求FunctionRequirement.OVER_window_only来声明。

        如果累加器需要存储大量数据，org.apache.flink.table.api.dataview.ListView和org.apache.frink.table.api.dataview.MapView
        提供了在无限制数据场景中利用flink状态后端的高级功能。

        由于其中一些方法是可选的，或者可以重载，因此运行时通过生成的代码调用聚合函数方法。这意味着基类并不总是提供要由具体实现重写的签名。
        尽管如此，所有提到的方法都必须公开声明，而不是静态的，并且命名与上面提到的要调用的名称完全相同。



        Table Aggregate Functions 表聚合函数
        用户定义的表聚合函数（UDTAGG）将多行的标量值映射为零、一或多行（或结构化类型）。
        返回的记录可能由一个或多个字段组成。如果输出记录仅由单个字段组成，则可以省略结构化记录，并且可以发出标量值，该标量值将由运行时隐式包装成一行。

        与聚合函数类似，表聚合的行为以累加器的概念为中心。累加器是一种中间数据结构，用于存储聚合值，直到计算出最终聚合结果。

        对于每一组需要聚合的行，运行时将通过调用createAccumulator（）创建一个空的累加器。
        随后，为每个输入行调用函数的accumulate（…）方法来更新累加器。
        处理完所有行后，将调用函数的emitValue（…）或emitUpdateWithRetract（…）方法来计算并返回最终结果。

        在本例中，我们假设一个表包含有关饮料的数据。该表由三列（id、name和price）和5行组成。
        我们想在表中找到所有饮料的2个最高价格，即执行TOP2（）表聚合。我们需要考虑5行中的每一行。结果是一个包含前两个值的表。


        为了定义表聚合函数，必须扩展org.apache.flink.table.functions中的基类TableAggregateFunction，
        并实现一个或多个名为accumulate（…）的求值方法。accumulat方法必须公开声明，而不是静态声明。
        Accumulate方法也可以通过实现多个名为Accumulate的方法来重载。


        默认情况下，使用反射自动提取输入、累加器和输出数据类型。
        这包括用于确定累加器数据类型的类的一般参数ACC和用于确定累加器数据类型地一般参数T。输入参数派生自一个或多个accumulate（…）方法。

        示例：
        import org.apache.flink.api.java.tuple.Tuple2;
        import org.apache.flink.table.api.*;
        import org.apache.flink.table.functions.TableAggregateFunction;
        import org.apache.flink.util.Collector;
        import static org.apache.flink.table.api.Expressions.*;

        // mutable accumulator of structured type for the aggregate function
        public static class Top2Accumulator {
          public Integer first;
          public Integer second;
        }

        // function that takes (value INT), stores intermediate results in a structured
        // type of Top2Accumulator, and returns the result as a structured type of Tuple2<Integer, Integer>
        // for value and rank
        public static class Top2 extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Accumulator> {

          @Override
          public Top2Accumulator createAccumulator() {
            Top2Accumulator acc = new Top2Accumulator();
            acc.first = Integer.MIN_VALUE;
            acc.second = Integer.MIN_VALUE;
            return acc;
          }

          public void accumulate(Top2Accumulator acc, Integer value) {
            if (value > acc.first) {
              acc.second = acc.first;
              acc.first = value;
            } else if (value > acc.second) {
              acc.second = value;
            }
          }

          public void merge(Top2Accumulator acc, Iterable<Top2Accumulator> it) {
            for (Top2Accumulator otherAcc : it) {
              accumulate(acc, otherAcc.first);
              accumulate(acc, otherAcc.second);
            }
          }

          public void emitValue(Top2Accumulator acc, Collector<Tuple2<Integer, Integer>> out) {
            // emit the value and rank
            if (acc.first != Integer.MIN_VALUE) {
              out.collect(Tuple2.of(acc.first, 1));
            }
            if (acc.second != Integer.MIN_VALUE) {
              out.collect(Tuple2.of(acc.second, 2));
            }
          }
        }

        TableEnvironment env = TableEnvironment.create(...);

        // call function "inline" without registration in Table API
        env
          .from("MyTable")
          .groupBy($("myField"))
          .flatAggregate(call(Top2.class, $("value")))
          .select($("myField"), $("f0"), $("f1"));

        // call function "inline" without registration in Table API
        // but use an alias for a better naming of Tuple2's fields
        env
          .from("MyTable")
          .groupBy($("myField"))
          .flatAggregate(call(Top2.class, $("value")).as("value", "rank"))
          .select($("myField"), $("value"), $("rank"));

        // register function
        env.createTemporarySystemFunction("Top2", Top2.class);

        // call registered function in Table API
        env
          .from("MyTable")
          .groupBy($("myField"))
          .flatAggregate(call("Top2", $("value")).as("value", "rank"))
          .select($("myField"), $("value"), $("rank"));


        Top2类的accumulate（…）方法接受两个输入。第一个是累加器，第二个是用户定义的输入。
        为了计算结果，累加器需要存储已累积的所有数据中的2个最高值。
        累加器由Flink的检查点机制自动管理，并在失败时进行恢复，以确保只有一次语义。结果值与排名索引一起发布。


        以下方法对于每个TableAggregateFunction都是强制性的：
        * createAccumulator()
        * accumulate(...)
        * emitValue(...) or emitUpdateWithRetract(...)

        此外，还有一些方法可以选择性地实现。虽然其中一些方法允许系统更高效地执行查询，但其他方法对于某些用例是强制性的。
        例如，如果聚合函数应应用于会话组窗口的上下文中，则merge（…）方法是强制性的（当观察到“连接”两个会话窗口的行时，需要连接它们的累加器）。

        public void accumulate(ACC accumulator, [user defined inputs])
        public void retract(ACC accumulator, [user defined inputs])
        public void merge(ACC accumulator, java.lang.Iterable<ACC> iterable)



        TableAggregateFunction的以下方法是必需的，具体取决于用例：
        * retract（…） ：对于OVER窗口上的聚合是必需的。
        * merge（…）：许多 bounded 聚合以及 unbounded session 窗口和Hop窗口聚合是必需的。
        * emitValue(...)：对于有界聚合和窗口聚合是必需的。

        ableAggregateFunction的以下方法用于提高流作业的性能：
        * emitUpdateWithRetract(...) ：用于发出已在收回模式下更新的值。

        emitValue（…）方法总是根据累加器发出完整的数据。在无限制的场景中，这可能会带来性能问题。以Top N函数为例。
        emitValue（…）每次都会发出所有N个值。为了提高性能，可以实现emitUpdateWithRetract（…），它以收回模式递增地输出数据。
        换句话说，一旦有更新，该方法就可以在发送新的更新记录之前收回旧记录。该方法将优先于emitValue（…）方法使用。

        如果表聚合函数只能在OVER窗口中应用，则可以通过在getRequirements（）中返回需求FunctionRequirement.OVER_window_only来声明。

        如果累加器需要存储大量数据，org.apache.flink.table.api.dataview.ListView和org.apache.frink.table.api.dataview.MapView
        提供了在无限制数据场景中利用flink状态后端的高级功能。

        由于某些方法是可选的或可以重载，因此这些方法由生成的代码调用。基类并不总是提供要由具体实现类重写的签名。
        尽管如此，所有提到的方法都必须公开声明，而不是静态的，并且命名与上面提到的要调用的名称完全相同。

        public void accumulate(ACC accumulator, [user defined inputs])
        public void retract(ACC accumulator, [user defined inputs])
        public void merge(ACC accumulator, java.lang.Iterable<ACC> iterable)
        public void emitValue(ACC accumulator, org.apache.flink.util.Collector<T> out)
        public void emitUpdateWithRetract(ACC accumulator, RetractableCollector<T> out)


        收回示例：
        以下示例显示如何使用emitUpdateWithRetract（…）方法仅发出增量更新。为了做到这一点，累加器同时保留旧的和新的前2个值。

        如果Top N中的N很大，则保留旧值和新值可能是低效的。
        解决这种情况的一种方法是在accumulate方法中仅将输入记录存储在累加器中，然后在emitUpdateWithRetract中执行计算。


        import org.apache.flink.api.java.tuple.Tuple2;
        import org.apache.flink.table.functions.TableAggregateFunction;

        public static class Top2WithRetractAccumulator {
          public Integer first;
          public Integer second;
          public Integer oldFirst;
          public Integer oldSecond;
        }

        public static class Top2WithRetract
            extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2WithRetractAccumulator> {

          @Override
          public Top2WithRetractAccumulator createAccumulator() {
            Top2WithRetractAccumulator acc = new Top2WithRetractAccumulator();
            acc.first = Integer.MIN_VALUE;
            acc.second = Integer.MIN_VALUE;
            acc.oldFirst = Integer.MIN_VALUE;
            acc.oldSecond = Integer.MIN_VALUE;
            return acc;
          }

          public void accumulate(Top2WithRetractAccumulator acc, Integer v) {
            if (v > acc.first) {
              acc.second = acc.first;
              acc.first = v;
            } else if (v > acc.second) {
              acc.second = v;
            }
          }

          public void emitUpdateWithRetract(
              Top2WithRetractAccumulator acc,
              RetractableCollector<Tuple2<Integer, Integer>> out) {
            if (!acc.first.equals(acc.oldFirst)) {
              // if there is an update, retract the old value then emit a new value
              if (acc.oldFirst != Integer.MIN_VALUE) {
                  out.retract(Tuple2.of(acc.oldFirst, 1));
              }
              out.collect(Tuple2.of(acc.first, 1));
              acc.oldFirst = acc.first;
            }
            if (!acc.second.equals(acc.oldSecond)) {
              // if there is an update, retract the old value then emit a new value
              if (acc.oldSecond != Integer.MIN_VALUE) {
                  out.retract(Tuple2.of(acc.oldSecond, 2));
              }
              out.collect(Tuple2.of(acc.second, 2));
              acc.oldSecond = acc.second;
            }
          }
        }

         */
    }

}
