package org.ditw.sparkplayground.sos.encoders

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.ditw.sparkplayground.utils.SparkallUtils

import scala.reflect.runtime.universe.TypeTag

case class IntBool(i: Int, b: Boolean)
case class StrStr(s1: String, s2: String)
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

  private def testIntBool(): Unit = {
    val intEncoder = implicitly[ExpressionEncoder[IntBool]]

    val slzr = intEncoder.createSerializer()
    println(s"(10, false): ${slzr.apply(IntBool(10, false))}")
    println(s"(-10, true): ${slzr.apply(IntBool(-10, true))}")
    val nul1 = slzr.apply(IntBool(10, false))
    nul1.setNullAt(0)
    nul1.setNullAt(1)
    println(s"(10, false) -> (null, null): $nul1")
    val nul2 = slzr.apply(IntBool(-10, true))
    nul2.setNullAt(0)
    println(s"(-10, true) -> (null, true): $nul2")
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

  private def testStr(): Unit = {
    val boolEncoder = implicitly[ExpressionEncoder[String]]

    val slzr = boolEncoder.createSerializer()
    println(s"Ok: ${slzr.apply("Ok")}")
    println(s" k: ${slzr.apply("k")}")
    println(s"  : ${slzr.apply("")}")
    val nul1 = slzr.apply("Ok")
    nul1.setNullAt(0)
    println(s"Ok -> null: $nul1")
    val nul2 = slzr.apply("k")
    nul2.setNullAt(0)
    println(s" k -> null: $nul2")
  }

  private def testStrStr(): Unit = {
    val boolEncoder = implicitly[ExpressionEncoder[StrStr]]

    val slzr = boolEncoder.createSerializer()
    println(s"('Ok', 'Now'): ${slzr.apply(StrStr("Ok", "Now"))}")
    println(s"('Ok',    ''): ${slzr.apply(StrStr("Ok", ""))}")
    println(s"('OkOkOkO', 'Now'): ${slzr.apply(StrStr("OkOkOkO", "Now"))}")
    println(s"('OkOkOkOk', 'Now'): ${slzr.apply(StrStr("OkOkOkOk", "Now"))}")
    println(s"('OkOkOkOkO', 'Now'): ${slzr.apply(StrStr("OkOkOkOkO", "Now"))}")
    val nul1 = slzr.apply(StrStr("Ok", "Now"))
    nul1.setNullAt(0)
    nul1.setNullAt(1)
    println(s"('Ok', 'Now') -> (null, null): $nul1")
    val nul2 = slzr.apply(StrStr("Ok", ""))
    nul2.setNullAt(0)
    println(s"('Ok',    '') -> (null, ''): $nul2")

    // val dszr = boolEncoder.createDeserializer()
    val dszr = boolEncoder.resolveAndBind().createDeserializer()
    val strstr1 = slzr.apply(StrStr("Ok", "Now"))
    val strstr1Back = dszr.apply(strstr1)
    assert(strstr1Back == StrStr("Ok", "Now"))

  }

  private def testStrStrComments(): Unit = {
    val boolEncoder = implicitly[ExpressionEncoder[StrStr]]

    // def createSerializer(): Serializer[T] = new Serializer[T](optimizedSerializer)
    // class Serializer[T](private val expressions: Seq[Expression])
    //    extends (T => InternalRow) with Serializable { ... }
    val slzr = boolEncoder.createSerializer()

    // def createDeserializer(): Deserializer[T] = new Deserializer[T](optimizedDeserializer)
    // class Deserializer[T](private val expressions: Seq[Expression])
    //    extends (InternalRow => T) with Serializable { ... }
    val dszr = boolEncoder.resolveAndBind().createDeserializer()

    // case class StrStr
    //  serializer:
    //    slzr = {ExpressionEncoder$Serializer@8580} <function1>
    //     expressions = {$colon$colon@11378} size = 2
    //      0 = {Alias@11384} staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(assertnotnull(input[0, org.ditw.sparkplayground.sos.encoders.StrStr, true])).s1, true, false, true) AS s1#6
    //      1 = {Alias@11385} staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(assertnotnull(input[0, org.ditw.sparkplayground.sos.encoders.StrStr, true])).s2, true, false, true) AS s2#7
    //     inputRow = {GenericInternalRow@11379} [StrStr(Ok,)]
    //     extractProjection = {GeneratedClass$SpecificUnsafeProjection@11380} "<function1>"
    //  deserializer:
    //    dszr = {ExpressionEncoder$Deserializer@8582} <function1>
    //     expressions = {$colon$colon@11351} size = 1
    //      0 = {NewInstance@11353} newInstance(class org.ditw.sparkplayground.sos.encoders.StrStr)
    //     constructProjection = null
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkallUtils.localSession()

    testBool()
    testInt()
    testLong()

    println("=---------------")
    testIntBool()
    println("=---------------")
    testStr()
    println("=---------------")
    testStrStr()

    spark.stop

  }
}
