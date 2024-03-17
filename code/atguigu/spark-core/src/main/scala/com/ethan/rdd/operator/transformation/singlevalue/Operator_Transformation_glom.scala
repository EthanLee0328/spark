package com.ethan.rdd.operator.transformation.singlevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_glom {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_MapPartitionsWithIndex_DataAndPar")

    val sparkContext = new SparkContext(sparkConf)


    //    val rdd: RDD[Int] = sparkContext.makeRDD(List(1, 2, 3, 4), 2)
    val rdd: RDD[Any] = sparkContext.makeRDD(List(List(List(1)), List(List(2)), List(List(3)), List(List(4)), List(List(5))), 2)

    val glomRDD: RDD[Array[Any]] = rdd.glom()

    //    println(glomRDD.collect().mkString(","))
    //    glomRDD.collect().foreach(data => println(data.mkString(",")))


    //    val mapRDD = glomRDD.map(
    //      array => {
    //        array.max
    //      }
    //    )


    //    println(mapRDD.collect().sum)//6

    glomRDD.collect().foreach(data => println(data.mkString(",")))


    sparkContext.stop()


  }
}
