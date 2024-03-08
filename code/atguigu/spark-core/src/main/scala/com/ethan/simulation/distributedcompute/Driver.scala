package com.ethan.simulation.distributedcompute

import java.io.ObjectOutputStream
import java.net.Socket


/**
 * @author EthanLee
 * @Version 1.0
 */
object Driver {
  def main(args: Array[String]): Unit = {

    val client01 = new Socket("localhost", 8888)
    val client02 = new Socket("localhost", 9999)

    val dataAndLogic = new DataAndLogic()
    val task01 = new Task()
    task01.logic = dataAndLogic.logic
    val task02 = new Task()
    task02.logic = dataAndLogic.logic

    task01.dataSet = dataAndLogic.dataSet.take(2)
    task02.dataSet = dataAndLogic.dataSet.takeRight(2)


    val out01 = client01.getOutputStream
    val out02 = client02.getOutputStream
    val objOut01 = new ObjectOutputStream(out01)
    val objOut02 = new ObjectOutputStream(out02)


    objOut01.writeObject(task01)
    objOut02.writeObject(task02)
    objOut01.flush()
    objOut02.flush()
    objOut01.close()
    objOut02.close()

    client01.close()
    client02.close()

    println("Driver has sent data to Executor01 and Executor02")
  }
}
