package com.ethan.rdd.operator.transformation.doublevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_DoubleValue_Subtract {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_DoubleValue_Subtract")
    val sparkContext = new SparkContext(sparkConf)

    val rdd01 = sparkContext.makeRDD(List(1, 2, 3, 4, 5), 2)
    val rdd02 = sparkContext.makeRDD(List(6, 7, 8, 9, 10), 3)
    val rdd03 = sparkContext.makeRDD(List("6", 7, 8, 9, 10), 3)

    rdd01.subtract(rdd02).collect().foreach(println)
   // rdd01.subtract(rdd03).collect().foreach(println)






    sparkContext.stop()


  }
}
