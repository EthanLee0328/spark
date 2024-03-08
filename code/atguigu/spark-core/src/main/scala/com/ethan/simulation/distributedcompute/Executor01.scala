package com.ethan.simulation.distributedcompute

import java.io.ObjectInputStream
import java.net.ServerSocket

/**
 * @author EthanLee
 * @Version 1.0
 */
object Executor01 {
  def main(args: Array[String]): Unit = {
    val executorServer01 = new ServerSocket(8888)
    println("Executor01 is ready to receive data from Driver")
    val executor01 = executorServer01.accept()
    val inputStream = executor01.getInputStream
    val objectInputStream = new ObjectInputStream(inputStream)
    val task = objectInputStream.readObject().asInstanceOf[Task]
    val result = task.compute()
    println("Executor01 has received data from Driver and computed the result" + result)
  }
}
