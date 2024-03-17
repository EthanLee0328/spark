package com.ethan.rdd.algorithm

/**
 * @author EthanLee
 *
 */

import org.apache.spark.util.random

import scala.util.Random

object RandomSampler {
  def sample[T](rdd: List[T], fraction: Double, withReplacement: Boolean = false, seed: Long = Random.nextLong()): List[T] = {
    val random = new Random(seed)
    var sampleResult: List[T] = List[T]()
    var remaining: List[T] = rdd

    // 计算期望采样数量，并将其转换为整数
    val expectedSampleSize = if (withReplacement) (rdd.length * fraction).toInt else Math.ceil(rdd.length * fraction).toInt

    // 对RDD中的每个元素进行遍历
    for (_ <- 1 to expectedSampleSize) {
      // 生成一个0到1之间的随机数
      val randomNumber = random.nextDouble()
      println(s"随机数：$randomNumber")

      // 判断随机数是否小于等于采样比例
      if (randomNumber <= fraction) {
        // 如果小于等于采样比例，则随机选中该元素，将其添加到采样结果列表中
        val length = remaining.length
        println(s"length: $length")
        val randomIndex = random.nextInt(length)
        println(s"randomIndex: $randomIndex")
        sampleResult = sampleResult :+ remaining(randomIndex)
        println(s"sampleResult: $sampleResult")
        // 如果是无放回抽样，创建一个新的 RDD，不包含被选中的元素
        if (!withReplacement) {
          // 使用过滤方法来创建新的 RDD，不包含被选中的元素
          remaining = remaining.zipWithIndex.filter { case (_, idx) => idx != randomIndex }.map(_._1)
          println(s"remaining: $remaining")
        }
      }
    }

    // 返回采样结果
    sampleResult
  }

  def main(args: Array[String]): Unit = {
    val rdd = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    val sampledRDD = RandomSampler.sample(rdd, 0.4)

    println("Sampled RDD: " + sampledRDD.mkString(", "))
  }
}
