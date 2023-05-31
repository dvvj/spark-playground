package org.ditw.sparkall

import org.apache.spark.{SparkConf, SparkContext}
import org.ditw.sparkall.utils.SparkallUtils

object HelloSparkSQL {
  def main(args:Array[String]):Unit = {

    val context = SparkallUtils.localContext("Hello Spark SQL")

    val sum = context.parallelize(Array(1, 2))
      .sum()
    println(s"sum: $sum")

    context.stop()
  }

}