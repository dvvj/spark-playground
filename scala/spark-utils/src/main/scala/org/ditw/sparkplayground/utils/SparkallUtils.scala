package org.ditw.sparkplayground.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkallUtils {
  def context(appName: String, isLocal: Boolean): SparkContext = {
    val conf = new SparkConf().setAppName(appName)
    if (isLocal) {
      conf.setMaster("local[2]")
    }

    val result = new SparkContext(conf)
    result.setLogLevel("WARN")
    result
  }

  def localContext(appName:String): SparkContext = {
    context(appName, true)
  }

  def session(appName: String, isLocal: Boolean, exConfig: Map[String, String] = Map.empty): SparkSession = {
    val sessionBuilder = SparkSession.builder().appName(appName)
    if (isLocal) {
      sessionBuilder.config("spark.master", "local[2]")
    }

    exConfig.foreach(cfg => sessionBuilder.config(cfg._1, cfg._2))

    val result = sessionBuilder.getOrCreate()
    result.sparkContext.setLogLevel("WARN")
    result
  }

  def localSession(appName: String, exConfig: Map[String, String] = Map.empty): SparkSession = {
    session(appName, true, exConfig)
  }

  def calcPartitionSize[T](rdd: RDD[T]): RDD[(Int, Long)] = {
    rdd.mapPartitionsWithIndex { (idx, iterator) =>
      val size = iterator.toSeq.length
      Iterator(idx -> size)
    }
  }

  def calcPartitionIterableSize[T1, T2](rdd: RDD[(T1, Iterable[T2])]): RDD[(Int, Long)] = {
    rdd.mapPartitionsWithIndex { (idx, iterator) =>
      val size = iterator.map(_._2.toSeq.size).sum
      Iterator(idx -> size)
    }
  }

  def tracePartitionSize[T](rdd: RDD[T]): Unit = {
    val partitionSizes = calcPartitionSize(rdd)
    println(partitionSizes.sortBy(_._2).collect().mkString("\t", "\n\t", ""))
  }

  def tracePartitionIterableSize[T1, T2](rdd: RDD[(T1, Iterable[T2])]): Unit = {
    val partitionSizes = calcPartitionIterableSize(rdd)
    println(partitionSizes.sortBy(_._2).collect().mkString("\t", "\n\t", ""))
  }

  def run(job: SparkallRunner): Unit = {
    job.run()
  }
}
