package com.ethan.rdd.build

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object RDD_Memory_Data_To_Pars extends App with Serializable {

  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_Memory_Data_To_Pars")


  val sparkContext = new SparkContext(sparkConf)

  val rdd: RDD[Int] = sparkContext.makeRDD(List(1, 2, 3, 4,5),3)

  rdd.saveAsTextFile("output")

  sparkContext.stop()


}
