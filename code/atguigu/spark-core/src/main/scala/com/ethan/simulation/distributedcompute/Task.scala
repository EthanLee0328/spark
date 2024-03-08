package com.ethan.simulation.distributedcompute

/**
 * @author EthanLee
 * @Version 1.0
 */
class Task extends Serializable {
  var dataSet: List[Int] = _
  var logic: Int => Int = _

  def compute(): List[Int] = dataSet.map(logic)
}
