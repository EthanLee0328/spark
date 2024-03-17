package com.ethan.rdd.operator.transformation.singlevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_groupBy {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_groupBy")

    val sparkContext = new SparkContext(sparkConf)


    //    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 0), 2)
    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 0), 3)

    def groupFunction(num: Int): Int = {
      num % 2
    }

    val groupByRDD = rdd.groupBy(groupFunction)

    groupByRDD.collect().foreach(println)


    val rddS = sparkContext.makeRDD(List("hello", "hii", "spark", "flink"), 3)

    val groupByRDDS = rddS.groupBy(_.charAt(2))

    groupByRDDS.collect().foreach(println)


    sparkContext.stop()


  }
}
