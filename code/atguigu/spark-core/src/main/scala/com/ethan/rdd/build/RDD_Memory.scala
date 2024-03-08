package com.ethan.rdd.build

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object RDD_Memory {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_Memory")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext
      //      .parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
      .makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
      .map(_ * 2)
      .collect()
      .foreach(println)
    sparkContext.stop()


  }
}
