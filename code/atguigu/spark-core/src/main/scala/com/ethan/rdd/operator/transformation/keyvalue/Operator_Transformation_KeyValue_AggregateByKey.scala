package com.ethan.rdd.operator.transformation.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_KeyValue_AggregateByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_KeyValue_AggregateByKey")
    val sparkContext = new SparkContext(sparkConf)


    val originalRDD = sparkContext.makeRDD(List(("a", 1), ("a", 2),("b", 3), ("b", 4), ("b", 5), ("a", 6)),2)


    originalRDD.glom().foreach(array => println(array.mkString(",")))


//    val aggregateByKeyRDD = originalRDD.aggregateByKey(0)(
//      (x, y) => math.max(x, y),
//      (x, y) => x + y
//    )

    val aggregateByKeyRDD = originalRDD.aggregateByKey(0)(math.max, _ + _)



    aggregateByKeyRDD.collect().foreach(println)


  }
}
