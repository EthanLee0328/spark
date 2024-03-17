package com.ethan.rdd.algorithm

import sun.tools.jconsole.inspector.Utils

import scala.util.Random

/**
 * @author EthanLee
 *         X{n+1} = (a * X{n} + c) % m
 */
object LinearCongruentialGenerator {
  def main(args: Array[String]): Unit = {
    // 算法参数
    //    val seed = 123456789L //  固定种子值
    val seed = Math.abs(Random.nextInt()) // 不固定种子值

    val a = 1664525L // 常数a
    val c = 1013904223L // 常数c
    val m = Math.pow(2, 32).toLong // 模数m
    println(s"线性同余算法参数：a=$a, c=$c, m=$m")
    val count = 5 // 生成随机数的个数

    // 调用线性同余算法生成随机数
    linearCongruentialGenerator(seed, a, c, m, count)
  }

  /**
   * 使用线性同余算法生成随机数。
   * Generates random numbers using the linear congruential algorithm.
   * 此算法基于给定的参数生成伪随机数序列。
   * This algorithm produces a sequence of pseudo-random numbers based on the given parameters.
   *
   * @param seed  初始状态的种子值
   *              The seed value, representing the initial state
   * @param a     算法中的常数 'a'
   *              The constant 'a' in the algorithm
   * @param c     算法中的常数 'c'
   *              The constant 'c' in the algorithm
   * @param m     算法中的模数 'm'
   *              The modulus 'm' in the algorithm
   * @param count 要生成的随机数个数
   *              The number of random numbers to generate
   */

  def linearCongruentialGenerator(seed: Long, a: Long, c: Long, m: Long, count: Int): Unit = {
    var currentState = seed // 初始状态为种子值
    println(s"种子值为：$seed")

    for (i <- 1 to count) {
      // 根据递推式计算下一个随机数
      currentState = (a * currentState + c) % m
      // 将随机数归一化到[0, 1]之间
      val randomNum = currentState.toDouble / m
      // 输出随机数
      println(s"随机数 $i: $randomNum")
    }
  }

}
