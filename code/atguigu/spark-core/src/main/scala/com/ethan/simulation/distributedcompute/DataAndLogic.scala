package com.ethan.simulation.distributedcompute

/**
 * @author EthanLee
 * @Version 1.0
 */
class DataAndLogic extends Serializable {
  val dataSet = List(1, 2, 3, 4)
  val logic: Int => Int = _ * 2
}
