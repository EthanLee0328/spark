package com.ethan.rdd.operator.transformation.singlevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_Coalesce {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_Coalesce")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    // 原始rdd情况
    rdd.glom().collect().foreach(array => println(array.mkString(",")))

    /**
     * def coalesce(numPartitions: Int, shuffle: Boolean = false,
     *  partitionCoalescer: Option[PartitionCoalescer] = Option.empty)
     */
    val coalesceRDD01 = rdd.coalesce(2)

    coalesceRDD01.glom().collect().foreach(array => println(array.mkString(",")))

    val coalesceRDD02 = rdd.coalesce(2, true)

    coalesceRDD02.glom().collect().foreach(array => println(array.mkString(",")))


    /**
     * def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
     *  coalesce(numPartitions, shuffle = true)
     */
    val repartitionRDD = rdd.repartition(3)
    repartitionRDD.glom().collect().foreach(array => println(array.mkString(",")))


    sparkContext.stop()


  }
}
