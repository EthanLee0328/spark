package com.ethan.rdd.operator.transformation.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author EthanLee
 * @Version 1.0
 */
object Operator_Transformation_DoubleValue_PartitionBy {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Transformation_DoubleValue_PartitionBy")

    val sparkContext = new SparkContext(sparkConf)


    val rdd: RDD[Int] = sparkContext.makeRDD(List(1, 2, 3, 4), 2)

    //rdd.partitionBy//Cannot resolve symbol partitionBy
    // RDD 中并无方法 partitionBy
    val key_value_RDD = rdd.map((_, 1))
    //    key_value_RDD.collect().foreach(println)


    /**
     * 1  Extra functions available on RDDs of (key, value) pairs through an implicit conversion. 通过隐式转换，可以在键值对RDD上使用额外的函数。
     * partitionBy 属于 class PairRDDFunctions[K, V]
     *
     * 2   Return a copy of the RDD partitioned using the specified partitioner. 返回使用指定分区器分区的RDD的副本。
     * def partitionBy(partitioner: Partitioner): RDD[(K, V)] = self.withScope {
     * if (keyClass.isArray && partitioner.isInstanceOf[HashPartitioner]) {
     * throw new SparkException("HashPartitioner cannot partition array keys.")
     * }
     * if (self.partitioner == Some(partitioner)) {
     * self
     * } else {
     * new ShuffledRDD[K, V, V](self, partitioner)
     * }
     * }
     */

    /**
     * 3.1 partitioner: Partitioner
     * 3.1.1  Partitioner to use. 使用的分区器。
     * An object that defines how the elements in a key-value pair RDD are partitioned by key.这是一个定义键值对RDD中元素如何按键进行分区的对象
     * Maps each key to a partition ID, from 0 to `numPartitions - 1`. 这个函数将每个键映射到一个分区ID，范围从0到numPartitions - 1
     * Note that, partitioner must be deterministic, i.e. it must return the same partition id given the same partition key 分区器在处理相同的分区键时必须具有确定性。也就是说，如果你多次使用相同的分区键作为输入，分区器应该每次都返回相同的分区 ID。这是为了确保在分区数据时，相同的键值对始终被分配到同一个分区中，以维护数据的一致性和可预测性。
     *
     * abstract class Partitioner extends Serializable {
     * def numPartitions: Int
     * def getPartition(key: Any): Int
     * }
     * 3.1.2 两个实现类
     * RangePartitioner 用于排序
     * sortBy底层用到了
     * sortBy=>sortByKey=>new RangePartitioner(numPartitions, self, ascending)
     * HashPartitioner 用于分组
     *
     * A [[org.apache.spark.Partitioner]] that implements hash-based partitioning using Java's `Object.hashCode`.
     * Java arrays have hashCodes that are based on the arrays' identities rather than their contents,
     * so attempting to partition an RDD[Array[_]] or RDD[(Array[_], _)] using a HashPartitioner will produce an unexpected or incorrect result.
     * 这段注释解释了 org.apache.spark.Partitioner 的一个实现，它使用 Java 的 Object.hashCode 来实现基于哈希的分区。在此过程中，需要注意的是，Java 数组的 hashCode 基于数组的标识而不是其内容。因此，尝试使用 HashPartitioner 来对 RDD[Array[]] 或 RDD[(Array[], _)] 进行分区会产生意外或不正确的结果
     *
     * class HashPartitioner(partitions: Int) extends Partitioner {
     * require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")
     *
     * def numPartitions: Int = partitions
     *
     * def getPartition(key: Any): Int = key match {
     * case null => 0
     * case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
     * }
     *
     * override def equals(other: Any): Boolean = other match {
     * case h: HashPartitioner =>
     * h.numPartitions == numPartitions
     * case _ =>
     * false
     * }
     *
     * override def hashCode: Int = numPartitions
     * }
     */

    /**
     * 4 RDD 中有个隐式转换的 函数
     * The following implicit functions were in SparkContext before 1.3 and users had to
     * `import SparkContext._` to enable them. Now we move them here to make the compiler find
     * them automatically. However, we still keep the old functions in SparkContext for backward
     * compatibility and forward to the following functions directly.
     *
     *
     * 这段注释解释了在 Spark 1.3 之前，下面的隐式函数是在 SparkContext 中的，用户必须通过 import SparkContext._ 来启用它们。现在我们将它们移到这里以使编译器能够自动找到它们。然而，为了向后兼容，我们仍然在 SparkContext 中保留了旧函数，并直接转发到下面的函数
     * implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
     * (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
     * new PairRDDFunctions(rdd)
     * }
     *
     * }
     */


    /**
     * 哈希码值（HashCode）通常是一个整数，它是根据对象的内容计算得出的。在Java中，hashCode方法通常被重写以实现自定义的哈希码计算逻辑。哈希码值不是字符，而是用于标识对象的唯一性的整数值。在哈希表等数据结构中，哈希码值可以用来确定对象在数据结构中的存储位置。
     * def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }
     所以 1 3 一个分区 2 4一个分区
     */
//    val value = key_value_RDD.partitionBy(new HashPartitioner(2))
    val value = key_value_RDD.partitionBy(new HashPartitioner(4))

    value.saveAsTextFile("output")
    sparkContext.stop()

    /**
     *  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
     也就是说你用同一个类型的分区器，且分区数相同，那么就是相等的 那就等于没操作呀
     */



  }
}


