package com.ethan.rdd.operator.transformation.doublevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_DoubleValue_Intersection {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_DoubleValue_Intersection")
    val sparkContext = new SparkContext(sparkConf)

    val rdd01 = sparkContext.makeRDD(List(1,2,3,4))
    val rdd02 = sparkContext.makeRDD(List(3,4,5,6))
    val rdd03 = sparkContext.makeRDD(List("1","2",5,6))

    val intersectionRDD01 = rdd01.intersection(rdd02)
   // val intersectionRDD02 = rdd01.intersection(rdd03)//Cannot resolve overloaded method 'intersection'

    intersectionRDD01.collect().foreach(println)

    sparkContext.stop()

  }
}
