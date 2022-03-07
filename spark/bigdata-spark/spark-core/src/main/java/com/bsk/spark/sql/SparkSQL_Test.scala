package com.bsk.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL_Test {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    /**
     * 需求：各区域热门商品 Top3
     *
     * 热门商品是从【点击量】 的纬度来看的，计算【各个区域】前三大 热门商品，
     * 并备注上每个商品在主要城市中的分布比例，超过两个城市用其他显示。
     *
     * 一共三张表：1张用户行为表，1张城市表，1张产品表
     * user_visit_action
     * city_info
     * product_info
     *
     * 这个只完成了部分需求，备注信息还没完成！
     *
     * */
    spark.sql("use test")

    spark.sql(
      """
        |select
        |	*
        |from (
        |	select
        |		*,
        |		rank() over( partition by area order by clickCnt desc) as rank
        |	from (
        |		select
        |			area,
        |			product_name,
        |			count(*) as clickCnt
        |		from (
        |			select
        |				a.*,
        |				p.product_name,
        |				c.area,
        |				c.city_name
        |			from user_visit_action a
        |			join product_info p on a.click_product_id = p.product_id
        |			join city_info c on a.city_id = c.city_id
        |			where a.click_product_id > -1
        |		) t1 group by area,product_name
        |	) t2
        |) t3 where rank <= 3
        |""".stripMargin).show()

    spark.close()
  }
}
