package com.ethan.rdd.operator.transformation.singlevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_MapPartitionsWithIndex_DataAndPar {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_MapPartitionsWithIndex_DataAndPar")

    val sparkContext = new SparkContext(sparkConf)


    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4, 7, 8, 9, 10))


    val mapPartitionsWithIndex = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        iter.map(
          num => {
            ("分区号是：" + index, "数值是：" + num)
          }
        )

      }

    )
    mapPartitionsWithIndex.collect().foreach(println)


    sparkContext.stop()


  }
}
