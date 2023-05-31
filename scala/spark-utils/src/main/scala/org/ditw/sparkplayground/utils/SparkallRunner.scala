package org.ditw.sparkplayground.utils

import org.apache.spark.sql.SparkSession

abstract class SparkallRunner(val appName: String) {

  def process(session: SparkSession): Unit

  def run(): Unit = {
    val session = SparkallUtils.localSession(appName)

    process(session)

    session.stop()
  }

}

