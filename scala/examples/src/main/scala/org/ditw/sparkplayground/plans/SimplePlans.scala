package org.ditw.sparkplayground.plans

import org.apache.spark.sql.types.{DateType, DoubleType}
import org.ditw.sparkplayground.utils.SparkallUtils

object SimplePlans {
  def main(args: Array[String]): Unit = {
    val spark = SparkallUtils.localSession()

    import spark.implicits._
    val data = Seq(("jaggu", "", "Bhai", "2011-04-01", "M", 30000),
      ("Michael", "madhan", "", "2015-05-19", "M", 40000),
      ("Robert", "", "Rome", "2016-09-05", "M", 40000),
      ("santhi", "", "sagari", "2012-02-17", "F", 52000),
      ("satya", "sai", "kumari", "2012-02-17", "F", 50000))

    var df = data.toDF("first_name", "middle_name", "last_name", "date of joining", "gender", "salary")
    df.show(false)

    import org.apache.spark.sql.functions._
    df = df
      .withColumn("date of joining", (col("date of joining").cast(DateType)))
      .withColumn("full_name", concat_ws(" ", col("first_name"), col("middle_name"), col("last_name")))

    df.explain(true)

    spark.stop()
  }
}
