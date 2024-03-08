package com.ethan.rdd.build

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object RDD_File_Data_To_Pars extends App {
  // 1.创建SparkConf
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_File_Data_To_Pars")

  // 2.创建SparkContext
  val sparkContext = new SparkContext(sparkConf)

  // 3.创建RDD
  val rdd = sparkContext.textFile("data/wc/wc01.txt", 2)

  // 4.保存文件
  rdd.saveAsTextFile("output")

  // 5.关闭连接
  sparkContext.stop()
}
