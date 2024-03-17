package com.ethan.test

/**
 * @author EthanLee
 * @Version 1.0
 */
object HashCodeTest {
  def main(args: Array[String]): Unit = {
    val str1 ='a'
    val str2 ='f'
    println(str1.hashCode()%2)
    println(str2.hashCode()%2)
  }

}
