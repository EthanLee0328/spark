package com.ethan.rdd.operator.transformation.singlevalue

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_FlatMap_Scala_Flatten {

  def main(args: Array[String]): Unit = {


    val nestedList = List(List(List(List(1)), List(List(2))), List(List(List(3)), List(List(4))), List(List(List(5)), List(List(6))))

    nestedList.foreach(println)

    val flatten01_nestedList = nestedList.flatten
    println("------------")

    flatten01_nestedList.foreach(println)

    println("------------")

    nestedList.flatten.flatten.flatten.foreach(println)


  }
}
