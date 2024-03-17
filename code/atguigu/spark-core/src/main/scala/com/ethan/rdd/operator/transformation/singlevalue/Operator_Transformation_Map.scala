package com.ethan.rdd.operator.transformation.singlevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_Map {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_Map")

    val sparkContext = new SparkContext(sparkConf)


    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4))

    //
    //    def mapFunction(num: Int): Int = {
    //      num * 2
    //    }


    //    val mapRDD = rdd.map(mapFunction)

    //    val mapRDD = rdd.map(
    //      num => {
    //        num * 2
    //      }
    //    )

    //    val mapRDD = rdd.map(num => num * 2)
    val mapRDD = rdd.map(_ * 2)
    mapRDD.collect().foreach(println)


    sparkContext.stop()


  }


}
