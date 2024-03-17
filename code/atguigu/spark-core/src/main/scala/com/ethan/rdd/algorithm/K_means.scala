package com.ethan.rdd.algorithm

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.Random

/**
 * @author EthanLee
 * @Version 1.0
 */
object K_means {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KMeansExample").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 生成随机数据点RDD
    val points = sc.parallelize(Seq.fill(100)(Point(Random.nextDouble() * 100, Random.nextDouble() * 100)))

    // 创建KMeansAlgorithm对象并运行K-means算法
    val kmeansAlgorithm = new KMeansAlgorithm(sc)
    val clusters = kmeansAlgorithm.runKMeans(points, k = 3, maxIterations = 10)

    // 输出最终的簇结果
    clusters.foreach(println)

    // 关闭Spark上下文
    sc.stop()
  }
  // 定义数据点类 x: Double：数据点的x坐标。 y: Double：数据点的y坐标。
  case class Point(x: Double, y: Double)

  // 定义簇类 centroid: Point：簇的中心点。 points: Iterable[Point]：簇中的所有数据点。
  case class Cluster(centroid: Point, points: Iterable[Point])

  // K-means算法类
  class KMeansAlgorithm(sparkContext: SparkContext) extends Serializable {
    // 计算两个点之间的欧氏距离
    def distance(point1: Point, point2: Point): Double = {
      math.sqrt(math.pow(point1.x - point2.x, 2) + math.pow(point1.y - point2.y, 2))
    }

    // 分配数据点到最近的簇
    def assignToCluster(point: Point, clusters: Array[Cluster]): Cluster = {
      val closestCluster = clusters.minBy(cluster => distance(point, cluster.centroid))
      closestCluster.copy(points = closestCluster.points ++ Iterable(point))
    }
    // 计算簇的新中心
    def updateCentroids(cluster: Cluster): Cluster = {
      val newCentroidX = cluster.points.map(_.x).sum / cluster.points.size
      val newCentroidY = cluster.points.map(_.y).sum / cluster.points.size
      Cluster(Point(newCentroidX, newCentroidY), Iterable.empty[Point])
    }


    // K-means算法
    def runKMeans(data: RDD[Point], k: Int, maxIterations: Int): Array[Cluster] = {
      var clusters = Array.fill(k)(Cluster(Point(Random.nextDouble() * 100, Random.nextDouble() * 100), Iterable.empty[Point]))

      var iteration = 0
      var hasChanged = true

      while (iteration < maxIterations && hasChanged) {
        val newClusters = data.map(point => assignToCluster(point, clusters)).collect()
        hasChanged = !clusters.zip(newClusters).forall { case (oldCluster, newCluster) =>
          oldCluster.centroid == newCluster.centroid && oldCluster.points == newCluster.points
        }
        clusters = newClusters.map(updateCentroids)
        iteration += 1
      }

      clusters
    }

  }


}
