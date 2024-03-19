package com.ethan.rdd.operator.transformation.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_KeyValue_ReduceByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_KeyValue_ReduceByKey")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4), ("b", 5), ("b", 6), ("b", 7), ("b", 8)))

//    rdd.reduceByKey(_ + _).collect().foreach(println)


    val value = rdd.reduceByKey(
      (x, y) => {
        println(s"x = $x, y = $y")
        x + y
      }
    )
    value.collect().foreach(println)
    sparkContext.stop()


  }

}
