package com.ethan.rdd.operator.transformation.singlevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_FlatMap_ETL02 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_MapPartitionsWithIndex_DataAndPar")

    val sparkContext = new SparkContext(sparkConf)


    val rdd = sparkContext.makeRDD(List(List(List(List(1)), List(List(2))), List(List(List(3)), List(List(4))), List(List(List(5)), List(List(6)))))


    //    val flatMapRDD = rdd.flatMap(
    //      list => {
    //        list
    //      }
    //    )
    // 固定深度的非递归扁平化
    val flatMapRDD = rdd.flatMap(_.flatMap(_.flatten))

    flatMapRDD.collect().foreach(println)
    sparkContext.stop()


  }
}
