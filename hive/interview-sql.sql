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







