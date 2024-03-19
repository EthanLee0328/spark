package com.ethan.rdd.operator.transformation.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_KeyValue_AggregateKey_Avg {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_KeyValue_AggregateKey_Avg")
    val sparkContext = new SparkContext(sparkConf)


    val originalRDD = sparkContext.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)


    originalRDD.glom().foreach(array => println(array.mkString(",")))


    val aggregateRDD = originalRDD.aggregateByKey((0, 0))(
      (t, v) => (t._1 + v, t._2 + 1),
      (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2)
    )
    aggregateRDD.mapValues(t => t._1 / t._2).collect().foreach(println)



    sparkContext.stop()


  }
}
