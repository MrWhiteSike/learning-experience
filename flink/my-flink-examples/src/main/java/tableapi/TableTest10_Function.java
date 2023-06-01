package tableapi;

public class TableTest10_Function {
    public static void main(String[] args) {
        /*
        Functions 函数

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





         */
    }

}
