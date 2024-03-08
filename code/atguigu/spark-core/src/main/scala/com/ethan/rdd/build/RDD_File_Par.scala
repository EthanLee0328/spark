package com.ethan.rdd.build

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object RDD_File_Par extends App {
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_File_Par")

  val sparkContext = new SparkContext(sparkConf)

  // 3.创建RDD
  val rdd = sparkContext.textFile("data/wc/wc01.txt",2)

  // 4.保存文件
  rdd.saveAsTextFile("output")

  // 5.关闭连接
  sparkContext.stop()
}
