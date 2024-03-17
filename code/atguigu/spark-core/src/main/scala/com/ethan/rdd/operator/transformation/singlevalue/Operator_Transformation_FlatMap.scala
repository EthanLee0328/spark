package com.ethan.rdd.operator.transformation.singlevalue

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_FlatMap {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_MapPartitionsWithIndex_DataAndPar")

    val sparkContext = new SparkContext(sparkConf)


    val rdd = sparkContext.makeRDD(
      List(
        List(1, 2),
        List(
          List(
            List(3, 4),
            List(5, 6)
          ),
          List(
            List(7, 8),
            List(9, 10)
          )
        )
      )
    )
    //
    //    val flatMapRDD = rdd.flatMap(
    //      list => {
    //        list
    //      }
    //    )

    //    val flatMapRDD = rdd.flatMap {
    //      case list: List[_] => list
    //      case nestedList: List[List[_]] => nestedList.flatten
    //    }
    //     直接在main方法中定义并使用递归深度扁平化的辅助函数
    val flatMapRDD = rdd.flatMap { list =>
      def flattenDeep(lst: Any, acc: ListBuffer[Int]): ListBuffer[Int] = lst match {
        case i: Int => acc += i
        case l: List[_] =>
          l.foreach(e => flattenDeep(e, acc))
          acc
        case _ => acc
      }

      flattenDeep(list, ListBuffer.empty[Int]).toSeq
    }


    flatMapRDD.collect().foreach(println)
    sparkContext.stop()


  }
}
