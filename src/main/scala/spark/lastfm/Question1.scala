package spark.lastfm

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

final case class User(id: String)

final case class RecentTrack(userId: String, timestamp: String, artistId: String, artistName: String, trackId: Option[String], trackName: String)

object Question1 extends Setup {
  override def run(): Unit = {
    val sc = createContext

    def recentTracks = parseRecentTracks(sc)
    val countDistinctTracksForUsers = transform(recentTracks)
    save(countDistinctTracksForUsers)

    sc.stop()
  }

  def transform(recentTracks: RDD[RecentTrack]) = {

    def tracksPlayedByUser = recentTracks.groupBy(_.userId)

    // TODO could this be turned into a for-comprehension
    tracksPlayedByUser.map {
      case (userId, tracks) =>
        val numberOfDistinctTracks = tracks.toSeq.distinct.size
        s"$userId\t$numberOfDistinctTracks"
    }
  }

  override protected def save(result: RDD[String]): Unit = result.saveAsTextFile("question1.tsv")
}
