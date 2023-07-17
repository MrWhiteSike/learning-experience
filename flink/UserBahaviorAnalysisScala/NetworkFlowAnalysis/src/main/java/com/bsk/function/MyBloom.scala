package com.bsk.function

/**
 * 自定义布隆过滤器
 * @param size
 */
case class MyBloom(size: Long){
  // 一般取的是2的整数次幂
  private val cap = size
  // 实现一个hash函数
  def hash(value: String, seed: Int): Long = {
    var result = 0L
    for (i <- 0 until value.length){
      result = result * seed + value.charAt(i)
    }
    (cap - 1) & result
  }
}

