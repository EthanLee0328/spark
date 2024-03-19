package com.ethan.rdd.operator.transformation.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_KeyValue_GroupByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_KeyValue_GroupByKey")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4), ("b", 5), ("b", 6), ("b", 7), ("b", 8), ("c", 9)))


    // 1 groupByKey
    val groupByKeyRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    groupByKeyRDD.collect().foreach(println)

    val groupByRDD: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
    groupByRDD.collect().foreach(println)


    sparkContext.stop()


  }

}
