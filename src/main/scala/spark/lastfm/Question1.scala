package spark.lastfm

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

final case class User(id: String)

final case class RecentTrack(userId: String, timestamp: String, artistId: String, artistName: String, trackId: Option[String], trackName: String)

object Question1 extends Setup {
  def run(): Unit = {
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

  private def parseRecentTracks(sc: SparkContext) = {
    val recentTrackData = {
      val filePath = "userid-timestamp-artid-artname-traid-traname.tsv"
      loadData(filePath, sc)
    }

    recentTrackData.map { line =>

      def emptyStringToNone: String => Option[String] = {
        case ""       => None
        case nonEmpty => Some(nonEmpty)
      }

      // This assumes every value is present.
      val delimited = line.split("\t")
      val userId = delimited(0)
      val timestamp = delimited(1)
      val artistId = delimited(2)
      val artistName = delimited(3)
      val trackId = emptyStringToNone(delimited(4))
      val trackName = delimited(5)
      RecentTrack(userId, timestamp, artistId, artistName, trackId, trackName)
    }
  }

  private def save(result: RDD[String]) = result.saveAsTextFile("question1.tsv")
}
