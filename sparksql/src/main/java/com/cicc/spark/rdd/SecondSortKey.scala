package com.cicc.spark.rdd


// 该类需要在网络中进行传输，需要序列化才可以，可以解决的方式：
// 1. with Serializable , 使用的时候 需要 new SecondSortKey
// 2.转变为case class， 就不需要with Serializable了， 使用的时候 不需要 new， 直接使用SecondSortKey创建即可
 class SecondSortKey(val word: String, val count: Int) extends Ordered[SecondSortKey] with Serializable{
  override def compare(that: SecondSortKey): Int = {
    if(this.word.compareTo(that.word) != 0){
      this.word.compareTo(that.word)
    }else{
      this.count - that.count
    }
  }
}
