package com.ethan.rdd.algorithm

/**
 * @author EthanLee
 * @Version 1.0
 */
object BitCounter {
  def main(args: Array[String]): Unit = {

    val num1: Long = 10 // 二进制表示为 1010 ==0101 ==0010==0001
    val num2: Long = 15 // 二进制表示为 1111

    println(s"Number of set bits in $num1: ${countSetBits(num1)}") // 输出 num1 中位为 1 的个数
    println(s"Number of set bits in $num2: ${countSetBits(num2)}") // 输出 num2 中位为 1 的个数

  }

  // 定义一个函数来计算二进制数中位为 1 的个数
  def countSetBits(num: Long): Int = {
    var count = 0
    var tempNum = num

    // 循环直到 tempNum 等于 0
    while (tempNum != 0) {
      // 每次与 1 进行与运算，如果结果为 1，则 count 加 1
      count += (tempNum & 1).toInt
      // 将 tempNum 右移一位，相当于除以 2
      tempNum >>= 1
    }

    // 返回计数结果
    count
  }

}
