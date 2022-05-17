interview

1、根据pid分类，统计最近3天、7天、30天的总用户数

uid
pid
dt
create table views(uid string, pid string, dt date);

1.根据pid分组，uid用户信息
select 
	pid,
	uid,
	dt
from views
where date_sub(CURRENT_DATE,interval 30 day) <= dt;t


2.统计最近30天，7天，3天的用户总数
select 
	t1.pid,
	t1.day30,
	t2.day7,
	t3.day3
from
	(select 
			pid,
			count(*) day30
		from views
		where date_sub(CURRENT_DATE,interval 30 day) <= dt)t1
left join
	(select 
			pid,
			count(*) day7
		from 
			views
		where date_sub(CURRENT_DATE,interval 7 day) <= dt
		group by pid)t2
on t1.pid = t2.pid
left join
	(select 
		pid,
		count(*) day3
	from 
		views
	where date_sub(CURRENT_DATE,interval 3 day) <= dt
	group by pid)t3
on t2.pid = t3.pid;


实现方式1：case when 语句可以解决这个多分支问题：

select 
	pid,
	count(*) day30,
	sum(case when date_sub(CURRENT_DATE,interval 7 day)<=dt then 1 else 0 end) day7,
	sum(case when date_sub(CURRENT_DATE,interval 3 day)<=dt then 1 else 0 end) day3
from views
where date_sub(CURRENT_DATE,interval 30 day) <= dt
group by pid;


select 
	pid,
	count(*) day30,
	sum(case when datediff(CURRENT_DATE,dt)<=7 then 1 else 0 end) day7,
	sum(case when datediff(CURRENT_DATE,dt)<=3 then 1 else 0 end) day3
from views
where date_sub(CURRENT_DATE,interval 30 day) <= dt
group by pid;

实现方式2： if + datediff
select 
	pid,
	count(*) day30,
	sum(if(datediff(CURRENT_DATE,dt)<=7 , 1 , 0)) day7,
	sum(if(datediff(CURRENT_DATE,dt)<=3 , 1 , 0)) day3
from views
where datediff(CURRENT_DATE,dt) <= 30
group by pid;


Q2：
揽收表字段
yundanID 运单号
uid 客户ID
dt 创建日期

需求：计算创建日期在0501-0531期间客户的单量分布情况

1.根据uid分组，计算每个用户的单数
select
	uid,
	count(*) ct
from
	lashou 
where dt >= '20200501' and dt <= '20200531'
group by uid;t1

2.根据用户单数，划分单量
select
	case 
	when ct <= 5 then '0-5'
	when ct >= 6 and ct <= 10 then '6-10'
	when ct >= 6 and ct <= 10 then '6-10'
	when ct >= 11 and ct <= 20 then '11-20'
	else '20以上' end as d_type
from
	t1;t2

3.根据单量进行分组，求count
select
	d_type,
	count(*) u_count
from
	t2
group by d_type;



最终SQL：
select
	d_type,
	count(*) u_count
from
	(select
		case 
		when ct <= 5 then '0-5'
		when ct >= 6 and ct <= 10 then '6-10'
		when ct >= 11 and ct <= 20 then '11-20'
		else '20以上' end as d_type
	from
		(select
			uid,
			count(*) ct
		from
			lashou 
		where dt >= '20200501' and dt <= '20200531'
		group by uid
		)t1
	)t2
group by d_type;



行列转换：
有数据表t1有以下数据内容：
stu_name  course  score
user1 语文 89
user1 数学 92
user1 英语 87
user2 语文 96
user2 数学 84
user2 英语 99

1.将course，score多行转一列
用户 课程 分数
user1 语文,数学,英语 89,92,87
user2 语文,数学,英语 96,84,99

根据用户名分组，通过concat_list将course，score多行转聚合成array类型，然后通过concat_ws将array根据指定分隔符进行拼接
select 
	stu_name,
	concat_ws(',',concat_list(course)) course,
	concat_ws(',',concat_list(score)) score 
from t1
group by stu_name; t2

2.将上面的结果再还原回去
select 
	stu_name,
	course_name,
	scores
from t2
lateral view explode(split(course,',')) temp1 as course_name
lateral view explode(split(score,',')) temp2 as scores;

注意：lateral view 使用多次，返回笛卡儿积，如果对两列都进行explode的话
，假设每列都有3个值，最终会变为3*3=9行，但是我们实际只想要3行

解决办法：
我们进行两次posexplode，课程和成绩都保留对应的序号，即便是变成了9行
通过where筛选只保留行号相同的index即可。

select 
	stu_name,
	course_name,
	scores
from t2
	lateral view posexplode(split(course,',')) temp1 as index_temp1,course_name
	lateral view posexplode(split(score,',')) temp2 as index_temp2,scores
where index_temp1 = index_temp2; t3

3.在t3基础上，对每个用户的课程成绩进行排名
可以使用rank() 函数（row_number函数对于相同的成绩也会赋予不同的排名，所以选择rank函数）
select 
	stu_name,
	course_name,
	scores,
	rank() over(partition by stu_name order by scores desc) as stu_rank
from t2
	lateral view posexplode(split(course,',')) temp1 as index_temp1,course_name
	lateral view posexplode(split(score,',')) temp2 as index_temp2,scores
where index_temp1 = index_temp2; t4



订单表t_order 

pay_state 1，已支付 2，已退款

月包

user_id order_id begin_time end_time pay_state last_updatetime

需求：求出用户最新更新的订单数据
select
	order_id
from
	(select
		user_id,
		order_id,
		row_number() over(partition by user_id order by last_updatetime desc) rn
	from t_order
	)t1
where rn = 1


求用户在某个日期的状态

t_status
user_id  status create_date
1 A  2021.10.2
1 B  2022.2.1
1 C  2022.4.18

用户1 在2022.3.1的状态
select user_id,status
from
	(select
			user_id,
			status,
			row_number() over(partition by user_id order by create_date desc) rn
		from t_status
		where create_date <= '2022.3.1') t1
where rn = 1


Q：（腾讯）登录日志，计算每个人连续登录的最大天数？
注意：断一天还算连续

 id      	dt
1001	2021-08-01
1001	2021-08-02
1001	2021-08-03
1001	2021-08-05
1001	2021-08-06
1001	2021-08-07
1001	2021-08-10
1001	2021-08-12
1002	2021-08-01
1002	2021-08-02
1002	2021-08-03
1002	2021-08-07
1002	2021-08-09
1002	2021-08-11
1002	2021-08-13
1002	2021-08-15

结果：
用户id    连续登录天数days
1001   			7
1002   			9

建表：
create table tx(id string, dt string) row format delimited fields terminated by '\t';
加载数据：
load data local inpath '/opt/module/data/tx.txt' into table tx;

查询数据：
select * from tx;

思路1： 等差数列（等差相同）

1.1 开窗，按照id分组同时按照dt排序,求Rank（注意：有数据重复要先去重，比如有两条
1001	2021-08-01）

create table tx(id int,dt string);

select
    id,
    dt,
    rank() over(partition by id order by dt) rk
from tx;t1

+---------+-------------+-------------+
| id  	  |     dt      |   rk  	  |
+---------+-------------+
| 1001    | 2021-08-01  |    1
| 1001    | 2021-08-02  |    2
| 1001    | 2021-08-03  |    3
| 1001    | 2021-08-05  |    4
| 1001    | 2021-08-06  |    5
| 1001    | 2021-08-07  |    6
| 1001    | 2021-08-10  |    7
| 1001    | 2021-08-12  |    8
| 1002    | 2021-08-01  |
| 1002    | 2021-08-02  |
| 1002    | 2021-08-03  |
| 1002    | 2021-08-07  |
| 1002    | 2021-08-09  |
| 1002    | 2021-08-11  |
| 1002    | 2021-08-13  |
| 1002    | 2021-08-15  |
+---------+-------------+

1.2 将每行日期减去rk值，如果之前是连续的日期，则相减之后为相同日期
select
    id,
    dt,
    date_sub(dt,rk) flag
from
    (select
    id,
    dt,
    rank() over(partition by id order by dt) rk
from tx)t1;t2

日期连续的减出来，flag值一样
+---------+-------------+-------------+
| id  	  |     dt      |  flag  	  |
+---------+-------------+
| 1001    | 2021-08-01  | 2021-07-31
| 1001    | 2021-08-02  | 2021-07-31
| 1001    | 2021-08-03  | 2021-07-31
| 1001    | 2021-08-05  | 2021-08-01
| 1001    | 2021-08-06  | 2021-08-01
| 1001    | 2021-08-07  | 2021-08-01
| 1001    | 2021-08-10  | 2021-08-03
| 1001    | 2021-08-12  | 2021-08-04

断一天的相减之后的，flag  值就连续了

1.3 先计算绝对连续的天数
select
    id,
    flag,
    count(*) days
from 
    (select
    id,
    dt,
    date_sub(dt,rk) flag
from
    (select
    id,
    dt,
    rank() over(partition by id order by dt) rk
from tx)t1)t2
group by id,flag; t3

+---------+-------------+-------------+
| id  	  |     flag    |  days  	  |
+---------+-------------+
| 1001    | 2021-07-31  | 3
| 1001    | 2021-08-01  | 3
| 1001    | 2021-08-03  | 1
| 1001    | 2021-08-04  | 1



1.4 再计算连续问题 （再次求rank）
select
    id,
    flag,
    days,
    rank() over(partition by id order by flag) newFlag
from
    t3;t4

+---------+-------------+-------------+
| id  	  |     flag    |  days  	  |  newFlag
+---------+-------------+
| 1001    | 2021-07-31  | 3                 1
| 1001    | 2021-08-01  | 3                 2
| 1001    | 2021-08-03  | 1                 3
| 1001    | 2021-08-04  | 1                 4


1.5 将flag 减去newFlag
select
    id,
    days,
    date_sub(flag,newFlag) flag
from
    t4;t5

+---------+-------------+-------------+
| id  	  |     flag    |  days  	  |
+---------+-------------+
| 1001    | 2021-07-30  | 3
| 1001    | 2021-07-30  | 3
| 1001    | 2021-07-31  | 1
| 1001    | 2021-07-31  | 1


1.6 计算每个用户连续登录的天数，断一天也算
select
    id,
    flag,
    sum(days)+count(*)-1 days
from
    t5
group by id,flag;t6


1.7 计算最大连续天数
select
    id,
    max(days)
from
    t6
group by id;

最终SQL：
select
    id,
    max(days)
from
    (select
    id,
    flag,
    sum(days)+count(*)-1 days
from
    (select
    id,
    days,
    date_sub(flag,newFlag) flag
from
    (select
    id,
    flag,
    days,
    rank() over(partition by id order by flag) newFlag
from
    (select
    id,
    flag,
    count(*) days
from 
    (select
    id,
    dt,
    date_sub(dt,rk) flag
from
    (select
    id,
    dt,
    rank() over(partition by id order by dt) rk
from tx)t1)t2
group by id,flag)t3)t4)t5
group by id,flag)t6
group by id;

注意：
-1 的问题：前面都是聚合函数，分组的时候，1 没有分组，所以报错。
解决：在最后进行 -1 操作，不影响最终结果

断两天也算呢，套两层
断五天呢，套5层
这种方法比较麻烦，嵌套很多层

思路2：分组思想，共同条件： 相减=1 || 相减=2的都可以

2.1 将上一行数据下移
select
    id,
    dt,
    lag(dt,1,'1970-01-01') over(partition by id order by dt) lagDt
from
    tx;t1

 id			dt		  lagdt
1001	2021-08-01	1970-01-01
1001	2021-08-02	2021-08-01
1001	2021-08-03	2021-08-02
1001	2021-08-05	2021-08-03
1001	2021-08-06	2021-08-05
1001	2021-08-07	2021-08-06
1001	2021-08-10	2021-08-07
1001	2021-08-12	2021-08-10
1002	2021-08-01	1970-01-01
1002	2021-08-02	2021-08-01
1002	2021-08-03	2021-08-02
1002	2021-08-07	2021-08-03
1002	2021-08-09	2021-08-07
1002	2021-08-11	2021-08-09
1002	2021-08-13	2021-08-11
1002	2021-08-15	2021-08-13

2.2 将当前行日期减去下移的日期
select
    id,
    dt,
    datediff(dt,lagDt) dtDiff
from
    t1;t2

+---------+-------------+-------------+
| id  	  |     dt      |  dtDiff  	  |
+---------+-------------+
| 1001    | 2021-08-01  | 18840 
| 1001    | 2021-08-02  | 1
| 1001    | 2021-08-03  | 1
| 1001    | 2021-08-05  | 2
| 1001    | 2021-08-07  | 1
| 1001    | 2021-08-06  | 1
| 1001    | 2021-08-10  | 3
| 1001    | 2021-08-12  | 2

分组的规律： >2 的开始做分组 + 1

2.3 分组
select
    id,
    dt,
    sum(if(dtDiff>2,1,0)) over(partition by id order by dt) flag
from
    t2;t3

+---------+-------------+-------------+
| id  	  |     dt      |  flag  	  |
+---------+-------------+
| 1001    | 2021-08-01  | 1
| 1001    | 2021-08-02  | 1
| 1001    | 2021-08-03  | 1
| 1001    | 2021-08-05  | 1
| 1001    | 2021-08-06  | 1
| 1001    | 2021-08-07  | 1
| 1001    | 2021-08-10  | 2
| 1001    | 2021-08-12  | 2


2.4 先对id,flag进行分组，然后组内取dt最大值和最小值，然后相减，最后差值+ 1，得到连续天数

select
    id,
    flag,
    datediff(max(dt),min(dt)) days
from
    t3
group by id,flag;t4


2.5 对id进行分组，取连续登录最大天数

select
	id,
	max(days)+1 as days
from 
    t4
group by id;


最终SQL：
select
    id,
    max(days)+1 as days
from
    (select
    id,
    flag,
    datediff(max(dt),min(dt)) days
from
    (select
    id,
    dt,
    sum(if(dtDiff>2,1,0)) over(partition by id order by dt) flag
from
    (select
    id,
    dt,
    datediff(dt,lagDt) dtDiff
from
    (select
    id,
    dt,
    lag(dt,1,'1970-01-01') over(partition by id order by dt) lagDt
from
    tx2)t1)t2)t3
group by id,flag)t4
group by id;


条件要变，
断两天：>3 即sum(if(dtDiff>3,1,0))
断三天：>4 即sum(if(dtDiff>4,1,0))


HiveOnSpark ： bug
	datediff over 并且 在子查询中 ===> NullPoint异常

	解决方案：
		1.换MR引擎
		2.将时间字段由string类型改为Date类型
			即建表的时候使用Date类型


days+1 days: 出现如下异常
Wrong arguments '1': No matching method for class org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPDTIPlus with (int, interval_day_time)

解决方案：
	1. days+1
	2. days+1 as days

注意：hive的客户端执行SQL语句时，SQL语句里不能存在 tab键，必须用4个空格键代替
原因：在Linux系统中tab键的功能是代码补全。




