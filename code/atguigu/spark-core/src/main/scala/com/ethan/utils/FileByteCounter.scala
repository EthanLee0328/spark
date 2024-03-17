package com.ethan.utils

import java.io.File

/**
 * @author EthanLee
 */
object FileByteCounter {
  def getFileSize(filePath: String): Option[Long] = {
    val file = new File(filePath)
    if (file.exists()) {
      Some(file.length())
    } else {
      None
    }
  }

  def main(args: Array[String]): Unit = {
    val filePath = "/Users/dz0400148/Desktop/spark/basic/atguigu/article/SparkCore.pdf"
    val fileSize = getFileSize(filePath)
    fileSize match {
      case Some(size) => println(s"File $filePath size is $size")
      case None => println(s"File $filePath not found")
    }
  }
}
