package org.ditw.sparkplayground.sos.encoders

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.ditw.sparkplayground.utils.SparkallUtils

import scala.reflect.runtime.universe.TypeTag

object EncoderTest1 {

  implicit def encoder[T: TypeTag]: ExpressionEncoder[T] = ExpressionEncoder()

  private def testBool(): Unit = {
    val boolEncoder = implicitly[ExpressionEncoder[Boolean]]

    val slzr = boolEncoder.createSerializer()
    println(s" true: ${slzr.apply(true)}")
    println(s"false: ${slzr.apply(false)}")
    val nul1 = slzr.apply(true)
    nul1.setNullAt(0)
    println(s" true -> null: $nul1")
    val nul2 = slzr.apply(false)
    nul2.setNullAt(0)
    println(s"false -> null: $nul2")

  }

  private def testInt(): Unit = {
    val intEncoder = implicitly[ExpressionEncoder[Int]]

    val slzr = intEncoder.createSerializer()
    println(s" 10: ${slzr.apply(10)}")
    println(s"-10: ${slzr.apply(-10)}")
    val nul1 = slzr.apply(10)
    nul1.setNullAt(0)
    println(s" 10 -> null: $nul1")
    val nul2 = slzr.apply(-10)
    nul2.setNullAt(0)
    println(s"-10 -> null: $nul2")

  }

  private def testLong(): Unit = {
    val intEncoder = implicitly[ExpressionEncoder[Long]]

    val slzr = intEncoder.createSerializer()
    println(s" 10L: ${slzr.apply(10L)}")
    println(s"-10L: ${slzr.apply(-10L)}")
    val nul1 = slzr.apply(10L)
    nul1.setNullAt(0)
    println(s" 10L -> null: $nul1")
    val nul2 = slzr.apply(-10L)
    nul2.setNullAt(0)
    println(s"-10L -> null: $nul2")

  }

  def main(args: Array[String]): Unit = {
    val spark = SparkallUtils.localSession()

    testBool()
    testInt()
    testLong()

    spark.stop

  }
}
