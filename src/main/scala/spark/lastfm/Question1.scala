package spark.lastfm

import org.apache.spark.rdd.RDD
import spark.lastfm.models.RecentTrack

object Question1 extends LastFm {

  override def run(): Unit = {
    withSparkContext("question1") { sc =>

      def recentTracks = parseRecentTracks(sc)

      val countDistinctTracksForUsers = transform(recentTracks)

      save(countDistinctTracksForUsers)
    }
  }

  def transform(recentTracks: RDD[RecentTrack]) =
    tracksPlayedByUser(recentTracks).map(countDistinctTracksForUser)

  def countDistinctTracksForUser: PartialFunction[(String, Iterable[RecentTrack]), String] = {
    case (userId, tracks) =>
      val numberOfDistinctTracks = tracks.toSeq.distinct.size
      s"$userId\t$numberOfDistinctTracks"
  }

  private def save(result: RDD[String]): Unit = result.saveAsTextFile("question1.tsv")
}
