package org.ditw.sparkplayground.bkHighPerf.ch2

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.ditw.sparkplayground.utils.SparkallUtils

object RDDIfaces {
  private def checkPartitions(rdd: RDD[Int]): Unit = {
    rdd.partitions.foreach { p => println(s"Partition # ${p.index}")}
    rdd.foreachPartition { ints =>
      val b = new StringBuilder()
      b.append("Partition ===\n")
      val l = ints.toList
      l.foreach(i => b.append(s"\t$i\n"))
      b.append(s"\tsum = ${l.sum}")
      println(b)
    }
  }

  private def checkDep(rdd: RDD[_]): Unit = {
    rdd.dependencies.foreach { d =>
      println(d)
    }
  }
  def main(args: Array[String]): Unit = {
    val sc = SparkallUtils.localContext()

    val rdd1 = sc.parallelize(1 to 20)
    println("------------------- rdd1")
    checkPartitions(rdd1)

    val rdd2 = rdd1.repartition(4)
    println("------------------- rdd2")
    checkPartitions(rdd2)
//    checkDep(rdd2)

    val rdd3 = rdd1.map(v => v -> v).partitionBy(new Partitioner {
      override def numPartitions: Int = 2

      override def getPartition(key: Any): Int = {
        key match {
          case i: Int => i % 2
          case _ => throw new IllegalArgumentException("Expecting int keys")
        }
      }
    })
    println("------------------- rdd3")
    checkPartitions(rdd3.values)
    checkDep(rdd3)


    sc.stop()
  }
}
