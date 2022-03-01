package com.bsk.spark.core.framework.service

import com.bsk.spark.core.framework.common.TService
import com.bsk.spark.core.framework.dao.WordCountDao

/**
 * 服务层
 */
class WordCountServeice extends TService{

  private val wordCountDao = new WordCountDao()

  // 数据分析
  def dataAnalysis() ={
    // TODO 执行业务操作
    val lines = wordCountDao.loadData("datas/1.txt")
    // 2、行解析，将行数据进行拆分
    val words = lines.flatMap(_.split(" "))
    // 3、将数据根据单词进行分组，便于统计
    val wordGroup = words.groupBy(word => word)
    // 4、对分组后的数据进行转换
    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    // 5、将数据结果采集到控制台打印
    val array = wordToCount.collect()
    array
  }

}
