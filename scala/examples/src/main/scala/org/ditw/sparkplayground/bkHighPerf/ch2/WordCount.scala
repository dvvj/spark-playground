package org.ditw.sparkplayground.bkHighPerf.ch2

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ditw.sparkplayground.utils.SparkallUtils

object WordCount {

  def wordCount1(sc: SparkContext, texts: Iterable[String]): RDD[(String, Int)] = {
    val words = sc.parallelize(texts.toSeq)
      .flatMap(_.split("\\s+"))
      .map(_.toLowerCase())
    val wordPairs = words.map(_ -> 1)
    wordPairs.reduceByKey(_ + _)
  }

  def wordCount2(sc: SparkContext, texts: Iterable[String], stopWords: Set[String]): RDD[(String, Int)] = {
    val words = sc.parallelize(texts.toSeq)
      .flatMap(_.split("\\s+"))
      .map(_.toLowerCase())
      .filter(w => !stopWords.exists(_.equalsIgnoreCase(w)))
    val wordPairs = words.map(_ -> 1)
    wordPairs.reduceByKey(_ + _)
  }

  def main(args: Array[String]): Unit = {
    val sc = SparkallUtils.localContext()

    val texts = Seq(
      "This is a book",
      "That is a tree",
      "The book is good"
    )

    val wc1 = wordCount1(sc, texts).collectAsMap()
    println(wc1)

    val wc2 = wordCount2(sc, texts, Set("this", "that", "is", "a")).collectAsMap()
    println(wc2)

    sc.stop()
  }
}
