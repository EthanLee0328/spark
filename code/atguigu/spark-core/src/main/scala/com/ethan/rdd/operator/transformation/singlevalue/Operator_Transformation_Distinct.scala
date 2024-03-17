package com.ethan.rdd.operator.transformation.singlevalue

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_Distinct {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_Distinct")

    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4), 1)

    //  map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)


    val tuples = rdd.map(x => (x, null))
    /**
     * (1,null)
     * (2,null)
     * (3,null)
     * (4,null)
     * (5,null)
     * (6,null)
     * (7,null)
     * (8,null)
     * (9,null)
     * (10,null)
     * (1,null)
     * (2,null)
     * (3,null)
     * (4,null)
     */

    val value1 = tuples.reduceByKey((x, _) => x)


    value1.collect().foreach(println)


    val value = rdd.distinct()


    println()

    rdd.partitions.foreach(println)

    value.collect().foreach(println)

  }

}
