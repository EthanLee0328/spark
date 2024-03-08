package com.ethan.rdd.build

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object RDD_File {
  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf并设置App名称
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_File")
    // 2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(sparkConf)

    // 3.创建RDD
    //    val fileRDD: RDD[String] = sc.textFile("data/wc")
    val fileRDD = sc.wholeTextFiles("data/wc")

    // 4.执行
    fileRDD.collect().foreach(println)

    // 5.关闭连接
    sc.stop()
  }

}
