package com.ethan.rdd.operator.transformation.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_KeyValue_PartitionBy_UserDefinePartitioner_BrowseDataProcessor {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_KeyValue_PartitionBy_UserDefinePartitioner_BrowseDataProcessor")
    val sparkContext = new SparkContext(sparkConf)
    // 模拟浏览数据
    val data = Seq(
      ("用户1", "体育"), ("用户2", "音乐"), ("用户3", "体育"),
      ("用户4", "色情"), ("用户5", "音乐"), ("用户6", "体育"),
      ("用户7", "音乐"), ("用户8", "体育"), ("用户9", "色情"),
      ("用户10", "体育"), ("用户11", "音乐"), ("用户12", "色情"),
      ("用户13", "音乐"), ("用户14", "体育"), ("用户15", "色情"),
      ("用户16", "音乐"), ("用户17", "体育"), ("用户18", "色情"),
      ("用户19", "音乐"), ("用户20", "体育"), ("用户21", "色情"),
      ("用户22", "音乐"), ("用户23", "体育"), ("用户24", "色情")
    )

    // 将数据转换为 RDD
    val rdd01 = sparkContext.parallelize(data)
    val rdd02 = rdd01.map(t => (t._2, t._1))




    // 使用匿名类直接传递给 partitionBy 方法的自定义分区器
    val partitionedRDD = rdd02.partitionBy(new org.apache.spark.Partitioner {
      override def numPartitions: Int = 4 // 设置分区数为4

      override def getPartition(key: Any): Int = key match {
        case "体育" => 0
        case "音乐" => 1
        case "色情" => 2
        case _ => 3 // 处理其他类别的数据
      }
    })

    val rdd03 = partitionedRDD.map(t => (t._2, t._1))



    // 输出分区结果
    rdd03.mapPartitionsWithIndex { (index, iter) =>
      iter.map(data => s"Partition $index: $data")
    }.collect().foreach(println)

    // 停止 SparkSession
    sparkContext.stop()




  }
}
