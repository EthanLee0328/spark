package com.ethan.simulation.distributedcompute

import java.io.ObjectInputStream
import java.net.ServerSocket

/**
 * @author EthanLee
 * @Version 1.0
 */
object Executor02 {
  def main(args: Array[String]): Unit = {
    val executorServer02 = new ServerSocket(9999)
    println("Executor02 is ready to receive data from Driver")
    val executor02 = executorServer02.accept()
    val inputStream = executor02.getInputStream
    val objectInputStream = new ObjectInputStream(inputStream)
    val task = objectInputStream.readObject().asInstanceOf[Task]
    val result = task.compute()
    println("Executor02 has received data from Driver and computed the result" + result)
  }
}
