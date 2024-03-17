package com.ethan.rdd.operator.transformation.singlevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 *          1 你给 一个rdd 一个期望值 一个随机种子数
 *          2 我先根据随机种子数生成一个随机数生成器
 *          3 然后根据期望值和随机数生成器生成一个随机数 挨着对比 合适的就放到采样结果中
 *          4 其实呢 就是生成随机概率然后和期望对比 合适的进入 然后再生成随机index 然后拿出合适的采样数据 所以每次都不定
 */
object Operator_Transformation_Sample01 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_Sample")

    val sparkContext = new SparkContext(sparkConf)


    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))


    val sampleRDD = rdd.sample(false, 0.4)


    println(sampleRDD.collect().mkString(","))


    sparkContext.stop()


  }


}
