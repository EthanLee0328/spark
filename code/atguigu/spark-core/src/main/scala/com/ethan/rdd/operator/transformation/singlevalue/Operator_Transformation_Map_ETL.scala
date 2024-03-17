package com.ethan.rdd.operator.transformation.singlevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_Map_ETL {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_Map")

    val sparkContext = new SparkContext(sparkConf)


    val rdd = sparkContext.textFile("data/log/apache.log")


    // rdd.collect().foreach(println)


    // ETL
    val etlRDD = rdd.map(
      line => {
        val data = line.split(" ")
        data(6)
      }
    )
    etlRDD.collect().foreach(println)

    sparkContext.stop()


  }


}
