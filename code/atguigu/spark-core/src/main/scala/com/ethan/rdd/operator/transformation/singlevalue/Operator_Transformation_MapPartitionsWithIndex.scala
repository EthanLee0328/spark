package com.ethan.rdd.operator.transformation.singlevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_MapPartitionsWithIndex {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_MapPartitionsWithIndex")

    val sparkContext = new SparkContext(sparkConf)


    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4), 2)

    val mapPartitionsWithIndex = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter.map(_ * 20)
        } else {
          iter
        }
      }
    )
    mapPartitionsWithIndex.collect().foreach(println)

    sparkContext.stop()


  }


}
