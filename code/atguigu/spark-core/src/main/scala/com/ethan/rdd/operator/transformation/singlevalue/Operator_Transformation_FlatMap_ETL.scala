package com.ethan.rdd.operator.transformation.singlevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_FlatMap_ETL {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_MapPartitionsWithIndex_DataAndPar")

    val sparkContext = new SparkContext(sparkConf)


    val rdd = sparkContext.makeRDD(List(List(1, 2), 3, List(4, 5)))

    //
    //    val flatMapRDD = rdd.flatMap(
    //
    //      data => {
    //        data match {
    //          case list: List[_] => list
    //          case dat => List(dat)
    //        }
    //      }
    //
    //    )

    val flatMapRDD = rdd.flatMap {
      case list: List[_] => list
      case dat => List(dat)
    }


    flatMapRDD.collect().foreach(println)
    sparkContext.stop()


  }
}
