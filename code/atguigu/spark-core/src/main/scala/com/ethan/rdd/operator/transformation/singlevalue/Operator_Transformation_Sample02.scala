package com.ethan.rdd.operator.transformation.singlevalue

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
 * @author EthanLee
 * @Version 1.0
 *          1 你给 一个rdd 一个期望值 一个随机种子数
 *          2 我先根据随机种子数生成一个随机数生成器
 *          3 然后根据期望值和随机数生成器生成一个随机数 挨着对比 合适的就放到采样结果中
 *          4 其实呢 就是生成随机概率然后和期望对比 合适的进入 然后再生成随机index 然后拿出合适的采样数据 所以每次都不定
 */
object Operator_Transformation_Sample02 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_Sample")

    val sparkContext = new SparkContext(sparkConf)


    // 模拟数据集，键值对表示用户ID和访问次数

    val rdd = sparkContext.makeRDD((1 to 100).map(id => (id.toString, Random.nextInt(1000))).toList)


    // 定义采样比例
    val fraction = 0.1

    // 用于存储每个用户的采样次数
    var userCounts = Map[String, Int]()
    // 定义采样次数
    val numSamples = 10

    // 采样十次
    for (i <- 1 to numSamples) {
      // 对数据集进行采样
      val sampledData = rdd.sample(false, fraction)

      // 统计采样结果中各用户的访问次数
      val sampledCounts = sampledData
        .map { case (userId, _) => (userId, 1) } // 将每个用户映射为 (userId, 1)
        .reduceByKey(_ + _) // 对相同用户的访问次数进行累加
        .collect() // 收集到Driver端

      // 更新每个用户的采样次数
      for ((userId, count) <- sampledCounts) {
        userCounts += userId -> (userCounts.getOrElse(userId, 0) + count)
      }
    }

    // 找出采样次数最多的前三个用户
    val topThreeUsers = userCounts.toList.sortBy(-_._2).take(3)

    // 输出结果
    println("Top three sampled users:")
    topThreeUsers.foreach(println)

    sparkContext.stop()


  }


}
