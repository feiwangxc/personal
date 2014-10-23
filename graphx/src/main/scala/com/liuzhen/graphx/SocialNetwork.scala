package com.liuzhen.graphx

import java.text.SimpleDateFormat

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object SocialNetwork {

  def main (args: Array[String]) {
    if (args.length != 4) {
      println("Usage: SocialNetwork <socialFile> <checkinFile> <maxFriendNum> <maxLocNum>")
    }

    val Array(socialFile, checkinFile, maxFriendNum, maxLocNum) = args
    val conf = new SparkConf().setAppName("SocialNetwork")
    val sc = new SparkContext(conf)

    // Calculating the social graph for 2nd-degree friends.
    val graph = getFriendGraph(GraphLoader.edgeListFile(sc, socialFile, false, 100), maxFriendNum.toInt)
    val friends = graph.vertices
      .flatMap { case (id, list) => list.map((id.toInt, _)) }
      .filter { case (userId, friendId) => userId != friendId }
      .distinct()

    friends.cache()
    println("Friends RDD Count: " + friends.count())

    // Reading checkin data.
    val checkins = getLatestCheckin(sc.textFile(checkinFile, 100), maxLocNum.toInt)
    checkins.cache()
    println("Checkin RDD Count: " + checkins.count())

    // Scoring each pair between user and friend.
    val pairs = friends.join(checkins).map {
      case (userId, (friendId, locMap)) => (friendId, (userId, locMap))
    }
    friends.unpersist()

    val pairScores = pairs.join(checkins).map {
      case (friendId, ((userId, userLocMap), friendLocMap)) =>
        val mutualLocSet = userLocMap.keySet.intersect(friendLocMap.keySet)
        if (mutualLocSet.isEmpty) {
          ((userId, friendId), (0, 0))
        } else {
          var inner = 0.0
          mutualLocSet.foreach(locId => inner += userLocMap(locId) * friendLocMap(locId))

          var mag1 = 0.0
          userLocMap.values.foreach(time => mag1 += time * time)

          var mag2 = 0.0
          friendLocMap.values.foreach(time => mag2 += time * time)

          ((userId, friendId), (mutualLocSet.size, inner / math.sqrt(mag1 * mag2)))
        }
    }

    pairScores.filter(_._2._1 > 0)
      .map(tuple => tuple._1._1 + "\t" + tuple._1._2 + "\t" + tuple._2._1 + "\t" + tuple._2._2)
      .saveAsTextFile("results")

    sc.stop()
  }

  def getFriendGraph(graph: Graph[Int, Int], maxFriendNum: Int) : Graph[List[Int], Int] = {
    val initGraph = graph.mapVertices((id, _) => List(id.toInt))

    initGraph.pregel(List[Int](), 2)(
      // Vertex program to update all vertices that receive messages.
      (id, data, msg) => if (msg.isEmpty) data else msg,

      // Sending messages.
      triplet => {
        if (triplet.srcId > triplet.dstId) {
          Iterator.empty
        } else {
          val msg = triplet.srcAttr.filter(id => id != triplet.dstId.toInt && !triplet.dstAttr.contains(id))
          if (msg.size > 0)
            Iterator((triplet.dstId, msg))
          else
            Iterator.empty
        }
      },

      // Merging messages.
      (a, b) => {
        if (a.size >= maxFriendNum)
          a
        else
          a ::: b.take(maxFriendNum - a.size)
      }
    )
  }

  def getLatestCheckin(input: RDD[String], maxLocNum: Int): RDD[(Int, Map[String, Long])] = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    val start = dateFormat.parse("2008-01-01T00:00:00Z").getTime / 1000

    val checkins = input.map(_.split("\t")).filter(_.length == 5).map(terms => {
      val userId = terms(0).toInt
      val locId = terms(4)
      val latitude = terms(2).toDouble
      val longitude = terms(3).toDouble

      if (latitude == 0 && longitude == 0) {
        ((userId, locId), 0L)
      } else {
        val time = dateFormat.parse(terms(1)).getTime / 1000 - start
        ((userId, locId), time)
      }
    })

    checkins.filter(_._2 > 0).reduceByKey((a, b) => a max b)
      .map(tuple => (tuple._1._1, Map(tuple._1._2 -> tuple._2)))
      .reduceByKey((a, b) => if (a.size >= maxLocNum) a else a ++ b.take(maxLocNum - a.size))
  }
}
