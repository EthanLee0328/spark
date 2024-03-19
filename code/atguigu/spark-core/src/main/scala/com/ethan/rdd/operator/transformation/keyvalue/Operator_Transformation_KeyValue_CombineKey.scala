package com.ethan.rdd.operator.transformation.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_KeyValue_CombineKey {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_KeyValue_CombineKey")

    val sparkContext = new SparkContext(sparkConf)

    val originalRDD = sparkContext.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)


    val combineByKeyRDD = originalRDD.combineByKey(
      v => (v, 1),
      (t: (Int, Int), v) => (t._1 + v, t._2 + 1),
      (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2)
    )
    combineByKeyRDD.mapValues(t => t._1 / t._2).collect().foreach(println)

    sparkContext.stop()


  }
}
