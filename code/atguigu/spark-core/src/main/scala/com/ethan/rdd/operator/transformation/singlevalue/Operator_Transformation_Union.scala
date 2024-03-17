package com.ethan.rdd.operator.transformation.singlevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_Union {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_Union")
    val sparkContext = new SparkContext(sparkConf)

    val rdd01 = sparkContext.makeRDD(List(1, 2, 3, 4, 5), 2)
    val rdd02 = sparkContext.makeRDD(List(6, 7, 8, 9, 10), 3)

    // 查看第一个 RDD 的分区情况

    println("rdd01 partitions:")
    rdd01.glom().collect().foreach(array => println(array.mkString(",")))

    // 查看第二个 RDD 的分区情况
    println("rdd02 partitions:")
    rdd02.glom().collect().foreach(array => println(array.mkString(",")))
    // 使用 union 方法合并两个 RDD
    val unionRDD = rdd01.union(rdd02)

    println("unionRDD partitions:")
    unionRDD.glom().collect().foreach(array => println(array.mkString(",")))


    sparkContext.stop()


  }
}
