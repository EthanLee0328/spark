package com.ethan.rdd.algorithm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object LogisticRegressionRDD {
  def main(args: Array[String]): Unit = {
    // 创建Spark配置
    val conf = new SparkConf().setAppName("LogisticRegressionRDD").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 创建RDD并加载数据
    val dataRDD = sc.textFile("data/algorithm/logicregession.csv") // 从CSV文件加载数据

    // 对数据进行预处理和转换，得到特征向量和标签
    val parsedDataRDD = dataRDD.map { line =>
      val parts = line.split(",") // 假设CSV文件以逗号分隔
      val label = parts(0).toDouble // 第一列是标签
      val features = parts.drop(1).map(_.toDouble) // 后续列是特征
      (label, features)
    }

    // 打印标签和特征数组
//    parsedDataRDD.collect().foreach { case (label, features) =>
//      println(s"Label: $label, Features: ${features.mkString("[", ", ", "]")}")
//    }



    // 定义预测函数
    def predict(features: Array[Double], weights: Array[Double]): Double = {
      val prediction = features.zip(weights).map { case (f, w) => f * w }.sum
      sigmoid(prediction)
    }

    // 定义sigmoid函数
    def sigmoid(z: Double): Double = 1.0 / (1.0 + Math.exp(-z))

    // 指定迭代次数和学习率
    val iterations = 100
    val alpha = 0.01

    // 定义梯度下降函数
    def gradientDescent(data: RDD[(Double, Array[Double])], iterations: Int, alpha: Double): Array[Double] = {
      val numFeatures = data.first()._2.length
      var weights = Array.fill(numFeatures)(0.0)

      for (_ <- 0 until iterations) {
        val grad = data.map { case (label, features) =>
          val prediction = predict(features, weights)
          val error = prediction - label
          features.map(_ * error) // 对每个特征乘以误差
        }.reduce((a, b) => a.zip(b).map { case (x, y) => x + y })

        weights = weights.zip(grad).map { case (w, g) => w - alpha * g / data.count() } // 更新权重
      }

      weights
    }


    // 运行梯度下降算法，得到模型权重
    val weights = gradientDescent(parsedDataRDD, iterations, alpha)

    // 打印模型权重
    println(s"Model weights: ${weights.mkString(", ")}")

    // 关闭Spark上下文
    sc.stop()
  }
}
