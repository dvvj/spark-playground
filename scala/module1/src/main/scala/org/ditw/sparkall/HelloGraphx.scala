package org.ditw.sparkall

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.ditw.sparkall.utils.SparkallUtils

import scala.collection.mutable

object HelloGraphx {
  def main(args:Array[String]):Unit = {

    val context = SparkallUtils.localContext("Hello Spark SQL")

    val users = context.parallelize(
      Seq(
        (3L, ("rxin", "student")),
        (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")),
        (2L, ("istoica", "prof"))
      )
    )

    val relationships = context.parallelize(
      Seq(
        Edge(3L, 7L, "collab"),
        Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"),
        Edge(5L, 7L, "pi"),
      )
    )

    val defUser = ("John Deo", "Missing")
    val graph = Graph(users, relationships, defUser)
    println(
      "postdoc #: " +
      graph.vertices.filter{
        case (_, (_, pos)) => pos == "prof"
      }.count()
    )

    val triStrs:RDD[String] = graph.triplets.map(tr => s"${tr.srcAttr._1} --[${tr.attr}]--> ${tr.dstAttr._1}")
    triStrs.foreach(println)
    println("In-degrees:")
    graph.inDegrees.foreach(println)
    println("Current graph:")
    graph.edges.foreach(println)
    println("New graph:")
    val newGraph = graph.mapTriplets(tr => s"${tr.srcAttr._1} --[${tr.attr}]--> ${tr.dstAttr._1}")
    newGraph.edges.foreach(println)

    println("aggregateMessages test1:")
    val t = newGraph.aggregateMessages[Int](
      _.sendToSrc(1), _ + _
    ).collect()
    println(t.mkString(","))
    val t2 = newGraph.aggregateMessages[String](
      ctxt => ctxt.sendToDst(ctxt.srcAttr._1), (m1, m2) => s"$m1-$m2"
    ).join(newGraph.vertices).collect()
    println(t2.mkString(","))

    calcDists(graph)

    context.stop()
  }

  def calcDists[TV, TE](graph: Graph[TV, TE]) = {
    val src2Dst = graph.aggregateMessages[List[Dist]](
      ctxt => ctxt.sendToSrc(List(Dist(ctxt.srcId, ctxt.dstId, 1))),
      (dists1, dists2) => {
        (dists1 ::: dists2)
          .groupBy(_.dstId)
          .mapValues(dists => dists.minBy(_.dist))
          .values.toList
      }
    )
    val t3 = src2Dst.collect()
    println(t3.map(p => s"${p._1}: ${p._2.map(d => d.dstId -> d.dist)}").mkString("\t", "\n\t", ""))

    val t2 = src2Dst.flatMap(p => p._2.map(d => d.dstId -> d)).rightOuterJoin(src2Dst).map { p =>
      val (newDist, currDists) = p._2
      if (newDist.isDefined) {
        newDist.get.srcId -> currDists.map(currDist => Dist(newDist.get.srcId, currDist.dstId, currDist.dist+newDist.get.dist))
      } else {
        p._1 -> currDists
      }
    }
    println("=== tt2")
    val tt2 = t2.collect()
    println(tt2.map(p => s"${p._1}: ${p._2.map(d => d.dstId -> d.dist)}").mkString("\t", "\n\t", ""))

  }

  case class Dist(srcId: VertexId, dstId: VertexId, dist: Int)
}
