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







         */
    }
}
