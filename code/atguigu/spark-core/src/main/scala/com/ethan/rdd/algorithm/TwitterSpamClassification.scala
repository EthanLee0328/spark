package com.ethan.rdd.algorithm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TwitterSpamClassification {
  def main(args: Array[String]): Unit = {
    // 创建Spark配置
    val conf = new SparkConf().setAppName("TwitterSpamClassification").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 模拟50GB的数据集，包含250,000个URL和107个特征/维度
    val data: RDD[(String, Array[Double])] = sc.parallelize(generateData(250000, 107))

    // 初始化模型参数
    val initialWeights: Array[Double] = Array.fill(107)(0.5)

    // 迭代次数
    val iterations = 10

    // 训练逻辑回归模型
    val finalWeights = trainLogisticRegression(data, initialWeights, iterations)

    // 打印最终的模型权重
    println("Final Weights:")
    finalWeights.foreach(println)

    // 停止Spark Context
    sc.stop()
  }

  // 生成模拟数据集，每个URL有107个特征/维度
  def generateData(numURLs: Int, numFeatures: Int): List[(String, Array[Double])] = {
    (1 to numURLs).map { i =>
      val url = s"url_$i"
      val features = Array.fill(numFeatures)(math.random)
      (url, features)
    }.toList
  }

  // 训练逻辑回归模型
  def trainLogisticRegression(data: RDD[(String, Array[Double])], initialWeights: Array[Double], iterations: Int): Array[Double] = {
    // 模拟逻辑回归训练过程
    var weights = initialWeights
    for (i <- 1 to iterations) {
      // 模拟计算梯度并更新权重
      weights = data.map { case (_, features) =>
        val gradient = computeGradient(features, weights)
        (1.0, gradient) // 将每个样本的梯度映射为(1.0, gradient)的形式，便于后续累加
      }.reduceByKey { case (grad1, grad2) =>
        addGradients(grad1, grad2) // 并行计算各样本的梯度总和
      }.map { case (_, gradientSum) =>
        updateWeights(weights, gradientSum, 0.01) // 更新权重
      }.collect()(0) // 收集并获取最新的权重
    }
    weights
  }

  // 计算梯度
  def computeGradient(features: Array[Double], weights: Array[Double]): Array[Double] = {
    val prediction = predict(features, weights)
    val error = 1.0 - prediction
    features.map(f => error * f)
  }

  // 预测
  def predict(features: Array[Double], weights: Array[Double]): Double = {
    val dotProduct = (features, weights).zipped.map(_ * _).sum
    sigmoid(dotProduct)
  }

  // Sigmoid函数
  def sigmoid(x: Double): Double = 1.0 / (1.0 + math.exp(-x))

  // 更新权重
  def updateWeights(weights: Array[Double], gradient: Array[Double], learningRate: Double): Array[Double] = {
    (weights, gradient).zipped.map { case (w, grad) =>
      w + learningRate * grad
    }
  }

  // 累加梯度
  def addGradients(grad1: Array[Double], grad2: Array[Double]): Array[Double] = {
    (grad1, grad2).zipped.map(_ + _)
  }
}
