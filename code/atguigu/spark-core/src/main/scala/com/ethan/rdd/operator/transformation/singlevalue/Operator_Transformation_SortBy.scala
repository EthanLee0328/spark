package com.ethan.rdd.operator.transformation.singlevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_SortBy {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_SortBy")
    val sparkContext = new SparkContext(sparkConf)
    val rdd = sparkContext.makeRDD(List(("1", 1), ("11", 2), ("2", 3), ("0", 4), ("02", 5), ("111", 6)), 3)
    //    rdd.collect().foreach(println)
    val sortByRDD = rdd.sortBy(_._1.toInt,ascending = false)
    sortByRDD.collect().foreach(println)
    sparkContext.stop()

  }

}
