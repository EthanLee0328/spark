package com.ethan.rdd.algorithm

/**
 * @author EthanLee
 *      假设有三个网页：A、B 和 C。

网页 A 链接到网页 B 和网页 C。
网页 B 链接到网页 A。
网页 C 链接到网页 A 和网页 B。
现在，我们要计算每个网页发送的贡献，以便在 PageRank 算法中更新它们的排名。

对于网页 A：

网页 A 链接到网页 B 和网页 C。
网页 A 的当前排名是 1.0。
因此，网页 A 对网页 B 和网页 C 的排名贡献是其排名（1.0）除以其出链数量（2），即 0.5。
所以，网页 A 发送给网页 B 和网页 C 的贡献分别是 0.5。
对于网页 B：

网页 B 链接到网页 A。
网页 B 的当前排名是 1.0。
因为网页 B 链接到网页 A，所以它对网页 A 的排名贡献是 1.0。
所以，网页 B 发送给网页 A 的贡献是 1.0。
对于网页 C：

网页 C 链接到网页 A 和网页 B。
网页 C 的当前排名是 1.0。
因为网页 C 链接到网页 A 和网页 B，所以它对这两个页面的排名贡献相等，即各为 0.5。
所以，网页 C 发送给网页 A 和网页 B 的贡献分别是 0.5。
这样，我们就计算出了每个网页发送的贡献，这些贡献将在 PageRank 算法的下一步中用于更新每个网页的排名。

 * */
import org.apache.spark.{SparkConf, SparkContext}

object PageRank {
  def main(args: Array[String]): Unit = {
    // Create a SparkContext
    // 创建一个SparkContext
    val conf = new SparkConf().setMaster("local[*]").setAppName("PageRank")
    val sc = new SparkContext(conf)

    // Load links data from text file and parse it into (URL, List[Outlink]) pairs
    // 从文本文件加载链接数据，并将其解析为（URL，List[Outlink]）对
    val links = sc.textFile("data/algorithm/pagerank.txt").map(line => {
      val tokens = line.split(" ")  // Split each line by space
      val page = tokens.head  // First token is the page name
      val outlinks = tokens.tail.toList  // Remaining tokens are outlinks
      (page, outlinks)
    })
//    links.collect().foreach(println)
//    (PageA,List(PageB, PageC))
//    (PageB,List(PageA, PageC))
//    (PageC,List(PageA))
      .persist()  // Persist the RDD in memory for reuse

    // Initialize ranks RDD with (URL, rank) pairs
    // 使用（URL，rank）对初始化ranks RDD
    var ranks = links.mapValues(_ => 1.0)  // Set initial rank for each page to 1.0
//    ranks.collect().foreach(println)
//    (PageA,1.0)
//    (PageB,1.0)
//    (PageC,1.0)

    // Set the number of iterations
    // 设置迭代次数
    val ITERATIONS = 100
    val alpha = 0.85
    val N = links.count()  // Get the total number of pages 3


//    val value = links.join(ranks)
//    在 join 操作中，Spark会匹配两个RDD中相同键的项，并将它们连接在一起，无需显式指定连接键。 Spark会自动推断出连接的键，并在内部进行连接操作。
//
//    value.collect().foreach(println)
//    (PageC,(List(PageA),1.0))
//    (PageA,(List(PageB, PageC),1.0))
//    (PageB,(List(PageA, PageC),1.0))


    // Compute contributions sent by each page
    // 计算每个页面发送的贡献
//    val contribs = value.flatMap {
//      case (page, (outlinks, rank)) =>
//        outlinks.map(dest => (dest, rank / outlinks.size))
//    }

//    contribs.collect().foreach(println)
//    (PageA,1.0)
//    (PageB,0.5)
//    (PageC,0.5)
//    (PageA,0.5)
//    (PageC,0.5)

//    val value1 = contribs.reduceByKey(_ + _)
//    value1.collect().foreach(println)
//    (PageC,1.0)
//    (PageA,1.5)
//    (PageB,0.5)

      // Sum contributions by URL
//      val value2 = value1.mapValues(sum => alpha / N + (1 - alpha) * sum)  // Update

//    value2.collect().foreach(println)
//    (PageC,0.43333333333333335)
//    (PageA,0.5083333333333333)
//    (PageB,0.35833333333333334)
    // Run PageRank algorithm for ITERATIONS iterations
    // 运行ITERATIONS次数的PageRank算法
    for (i <- 1 to ITERATIONS) {


      // Compute contributions sent by each page
      // 计算每个页面发送的贡献
      val contribs = links.join(ranks).flatMap {
        case (page, (outlinks, rank)) =>
          outlinks.map(dest => (dest, rank / outlinks.size))
      }
//       Sum contributions by URL and update ranks
//       按URL对贡献进行求和，并更新排名
      ranks = contribs.reduceByKey(_ + _)  // Sum contributions by URL
        .mapValues(sum => alpha / N + (1 - alpha) * sum)  // Update ranks using PageRank formula
    }
//
//    // Print the final ranks
//    // 打印最终排名
    ranks.collect().foreach(println)

    // Stop SparkContext
    // 停止SparkContext
    sc.stop()
  }
}
