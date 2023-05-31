package org.ditw.sparkplayground

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.ditw.sparkplayground.utils.SparkallUtils

object HelloSparkSQL {
  def main(args:Array[String]):Unit = {

    val session = SparkallUtils.localSession("Hello Spark SQL")
    import session.implicits._
    val rdd = session.sparkContext.parallelize(1 to 10).map(i => (i, s"#${i}"))
    val df = rdd.toDF("index", "value")
    println("========= schema: ")
    df.printSchema()
    println("========= data: ")
    df.show()

    val schema = StructType(Array(
      StructField("index", IntegerType, false),
      StructField("value", StringType, false)
    ))

    val df2 = session.createDataFrame(rdd.map(tp => Row(tp._1, tp._2)), schema)
    println("========= schema: ")
    df2.printSchema()
    println("========= data: ")
    df2.show()

    session.stop()
  }

}
