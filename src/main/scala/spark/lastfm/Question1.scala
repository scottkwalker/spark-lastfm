package spark.lastfm

import org.apache.spark.rdd.RDD
import spark.lastfm.models.RecentTrack

object Question1 extends LastFm {

  override def run(): Unit =
    withSparkContext("question1") { sc =>

      def recentTracks = parseRecentTracks(sc)

      val countDistinctTracksForUsers = transform(recentTracks)

      save(countDistinctTracksForUsers)
    }

  def transform(recentTracks: RDD[RecentTrack]) =
    tracksPlayedByUser(recentTracks)
      .map(countDistinctTracksForUser)
      .map(format)

  def countDistinctTracksForUser: PartialFunction[(String, Iterable[RecentTrack]), (String, Int)] = {
    case (userId, tracks) =>
      val numberOfDistinctTracks = tracks.toSeq.distinct.size
      (userId, numberOfDistinctTracks)
  }

  def format: PartialFunction[(String, Int), String] = {
    case (userId, numberOfDistinctTracks) =>
      s"$userId\t$numberOfDistinctTracks"
  }

  private def save(result: RDD[String]): Unit = result.saveAsTextFile("question1.tsv")
}
