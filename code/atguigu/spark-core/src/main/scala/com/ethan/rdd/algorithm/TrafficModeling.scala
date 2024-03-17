package com.ethan.rdd.algorithm

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TrafficModeling {
  // Define case class for road link and trip data
  case class RoadLink(linkId: Int, length: Double)
  case class TripData(tripId: Int, linkIds: List[Int], travelTime: Double)

  // Define functions for calculating travel time and link congestion
  def computeTravelTime(linkIds: List[Int], roadLinks: Array[RoadLink], sc: SparkContext): RDD[(Int, Double)] = {
    val linksRDD = sc.parallelize(roadLinks)
    linksRDD
      .filter(link => linkIds.contains(link.linkId))
      .map(link => (link.linkId, link.length))
  }

  def computeLinkCongestion(travelTime: Double, length: Double): Double = {
    travelTime / length
  }

  def main(args: Array[String]): Unit = {
    // Create Spark configuration
    val conf = new SparkConf().setAppName("TrafficModeling").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Sample data for road links and trip data
    val roadLinks = List(
      RoadLink(1, 10.5),
      RoadLink(2, 15.2),
      RoadLink(3, 8.7),
      RoadLink(4, 12.3)
    )

    val tripData = List(
      TripData(1, List(1, 2, 3), 25.6),
      TripData(2, List(2, 4), 20.3),
      TripData(3, List(1, 3, 4), 30.1),
      TripData(4, List(1, 2), 15.8)
    )

    // Convert roadLinksRDD to a serializable type
    val roadLinksArray = roadLinks.toArray

    // Create a broadcast variable for roadLinksRDD
    val broadcastRoadLinksRDD: Broadcast[Array[RoadLink]] = sc.broadcast(roadLinksArray)

    // Perform traffic modeling using RDD operations
    val roadLinkLengths = computeTravelTime(tripData.flatMap(_.linkIds), roadLinksArray, sc)
    val linkCongestionRDD = sc.parallelize(tripData).flatMap { trip =>
      val travelTime = computeTravelTime(trip.linkIds, roadLinksArray, sc)
      trip.linkIds.map(linkId => (linkId, (travelTime.lookup(linkId).head, roadLinkLengths.lookup(linkId).head)))
    }.groupByKey().mapValues { trips =>
      val totalTravelTime = trips.map(_._1).sum
      val totalLength = trips.map(_._2).sum
      computeLinkCongestion(totalTravelTime, totalLength)
    }

    // Collect and print the link congestion results
    val congestionResults = linkCongestionRDD.collect()
    congestionResults.foreach { case (linkId, congestion) =>
      println(s"Link $linkId congestion: $congestion")
    }

    // Stop the Spark context
    sc.stop()
  }
}
