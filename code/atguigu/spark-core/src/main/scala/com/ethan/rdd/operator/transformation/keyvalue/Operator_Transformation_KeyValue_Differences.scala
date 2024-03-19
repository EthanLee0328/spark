package com.ethan.rdd.operator.transformation.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_KeyValue_Differences {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_KeyValue_CombineKey")

    val sparkContext = new SparkContext(sparkConf)

    val originalRDD = sparkContext.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)

    // combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)
    originalRDD.reduceByKey(_ + _).collect().foreach(println)
    originalRDD.aggregateByKey(0)(_ + _, _ + _).collect().foreach(println)
    originalRDD.foldByKey(0)(_ + _).collect().foreach(println)
    originalRDD.combineByKey(v => v, (c: Int, v) => c + v, (c1: Int, c2: Int) => c1 + c2).collect().foreach(println)


    sparkContext.stop()


  }
}
