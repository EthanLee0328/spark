package com.ethan.scala

/**
 * @author EthanLee
 * @Version 1.0
 */
object ClosureDemo {
  def main(args: Array[String]): Unit = {
    /*
      Please write a program with the following requirements:
      1. Write a function makeSuffix(suffix: String) that accepts a file extension (e.g., ".jpg") and returns a closure.
      2. When the closure is called with a filename, if the filename does not have the specified extension (e.g., ".jpg"), it returns the filename with the extension appended (e.g., "dog" becomes "dog.jpg"). If the filename already has the ".jpg" extension, it returns the original filename.
         For example, if the filename is "dog" => returns "dog.jpg"
         For example, if the filename is "cat.jpg" => returns "cat.jpg"
      3. This should be accomplished using a closure.
         Hint: String.endsWith(xx)
    */
    val f = makeSuffix(".jpg")
    println(f("dog"))
    println(f("cat.jpg"))


  }

  def makeSuffix(suffix: String) = {
    (fileName: String) => {
      if (fileName.endsWith(suffix)) {
        fileName
      } else {
        fileName + suffix
      }
    }
  }


}
