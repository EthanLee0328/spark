package com.ethan.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(sparkConf)

    val fileRDD: RDD[String] = sc.textFile("data/wc")

    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    val word2OneRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    val word2CountRDD: RDD[(String, Int)] = word2OneRDD.reduceByKey(_ + _)
    val word2Count: Array[(String, Int)] = word2CountRDD.collect()
    word2Count.foreach(println)
    sc.stop()
  }

}
