package org.ditw.sparkplayground

import org.ditw.sparkplayground.utils.SparkallUtils

object DfApiVsRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkallUtils.localSession("DfApiVsRDD")

    import spark.implicits._
    val df1 = Seq(
      (1, "John", "Smith"),
      (2, "Joe", "Smith")
    ).toDF("id", "firstName", "lastName")

    df1.explain(true)
    df1.show()

    spark.stop()
  }
}
