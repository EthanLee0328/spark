package com.ethan.rdd.operator.transformation.singlevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_MapPartitions {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_Map")

    val sparkContext = new SparkContext(sparkConf)


    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4), 2)


    val mapPartitionsRDD = rdd.mapPartitions(
      iter => {
        println(">>>>>>>>>>")
        iter.map(_ * 2)
      }
    )

    mapPartitionsRDD.collect().foreach(println)


    sparkContext.stop()


  }


}
