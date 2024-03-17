package com.ethan.rdd.operator.transformation.doublevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_DoubleValue_Zip {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_DoubleValue_Intersection")
    val sparkContext = new SparkContext(sparkConf)

    val rdd01 = sparkContext.makeRDD(List(1,2,3,4))//Can't zip RDDs with unequal numbers of partitions: List(2, 8)
    val rdd02 = sparkContext.makeRDD(List(3,4,5,6))
    val rdd03 = sparkContext.makeRDD(List("11",4,5,6))
    val rdd04 = sparkContext.makeRDD(List("a",4,5,6,'a',true))//Can only zip RDDs with same number of elements in each partition

    rdd01.partitions.foreach(println)

    rdd01.glom().collect().foreach(array => println(array.mkString(",")))

//    rdd01.saveAsTextFile("output")

   rdd01.zip(rdd02).collect().foreach(println)
   rdd01.zip(rdd03).collect().foreach(println)
   rdd01.zip(rdd04).collect().foreach(println)


    sparkContext.stop()

  }
}
