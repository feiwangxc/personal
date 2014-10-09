package com.liuzhen.graphx

import org.apache.spark._
import org.apache.spark.graphx._

object SocialNetwork {

  def main (args: Array[String]) {
    if (args.length != 1) {
      println("Usage: SocialNetwork <dataPath>")
    }

    val dataPath = args(0)
    val conf = new SparkConf().setAppName("SocialNetwork")
    val sc = new SparkContext(conf)

    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, dataPath)
    val initGraph = graph.mapVertices((id, _) => List(id.toInt))

    val friendGraph = initGraph.pregel(List[Int](), 2)(
      (id, data, msg) => if (msg.isEmpty) data else msg,
      triplet => {
        val msg = triplet.srcAttr.filter(id => id != triplet.dstId.toInt && !triplet.dstAttr.contains(id))
        if (msg.size > 0)
          Iterator((triplet.dstId, msg))
        else
          Iterator.empty
      },
      (a, b) => {
        val maxSize = 20
        if (a.size >= maxSize)
          a
        else if (b.size >= maxSize)
          b
        else {
          val msg = a ::: b
          if (msg.size > maxSize) msg.drop(msg.size - maxSize) else msg
        }
      }
    )

    val results = friendGraph.vertices.filter(_._2.size > 0).collect()
    println("Count: " + results.length)
    println("Node " + results(0)._1.toInt + ": " + results(0)._2.mkString(","))

    sc.stop()
  }
}
