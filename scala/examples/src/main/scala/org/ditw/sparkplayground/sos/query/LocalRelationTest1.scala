package org.ditw.sparkplayground.sos.query

import org.ditw.sparkplayground.utils.SparkallUtils

object LocalRelationTest1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkallUtils.localSession()

    val seq1 = Seq(
      ("a", 1),
      ("bab", 14)
    )

    import spark.implicits._
    val df = seq1.toDF("name", "v")

    // 1. Seq -> DatasetHolder
    //  implicit def localSeqToDatasetHolder[T : Encoder](s: Seq[T]): DatasetHolder[T] = {
    //    DatasetHolder(_sqlContext.createDataset(s))
    //  }

    // 2. def createDataset[T : Encoder](data: Seq[T]): Dataset[T]
    //    val enc = encoderFor[T]
    //    val toRow = enc.createSerializer()
    //    val attributes = enc.schema.toAttributes
    //    val encoded = data.map(d => toRow(d).copy())
    //    val plan = new LocalRelation(attributes, encoded)
    //    Dataset[T](self, plan)

    // LocalRelation:
    //   The local collection holding the data. It doesn't need to be sent to executors
    //     and then doesn't need to be serializable.
    //  case class LocalRelation(
    //    output: Seq[Attribute],
    //    data: Seq[InternalRow] = Nil,
    //    // Indicates whether this relation has data from a streaming source.
    //    override val isStreaming: Boolean = false)
    //  extends LeafNode with analysis.MultiInstanceRelation {

    println(df.queryExecution)

    spark.stop()
  }

}
