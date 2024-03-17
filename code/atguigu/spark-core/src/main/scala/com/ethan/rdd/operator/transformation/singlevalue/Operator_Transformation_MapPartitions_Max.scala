package com.ethan.rdd.operator.transformation.singlevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_MapPartitions_Max {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_Map")

    val sparkContext = new SparkContext(sparkConf)


    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4), 10)


    val mapPartitionsRDD = rdd.mapPartitions(
      iter => {
        println(">>>>>>>>>>")
        List(if (iter.hasNext) iter.max else 0).iterator
      }
    )

    mapPartitionsRDD.collect().foreach(println)


    sparkContext.stop()


  }


}
