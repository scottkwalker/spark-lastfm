package spark.lastfm

import org.apache.spark.rdd.RDD
import spark.lastfm.models.RecentTrack

object Question3 extends LastFm {

  override def run(): Unit = {
    val sc = createContext

    def recentTracks = parseRecentTracks(sc)

    val countDistinctTracksForUsers = transform(recentTracks, limit = 10)
    save(countDistinctTracksForUsers)

    sc.stop()
  }

  def transform(recentTracks: RDD[RecentTrack], limit: Int) = {
    tracksPlayedByUser(recentTracks).map(format)

    // TODO filter not in session
    // TODO find longest sessions.
    // TODO use limit
  }

  def format: PartialFunction[(String, Iterable[RecentTrack]), String] = {
    case (userId, tracks) =>
      val sessionFirst = tracks.head
      val sessionLast = tracks.last
      val startTimestamp = sessionFirst.timestamp
      val endTimestamp = sessionLast.timestamp
      val songsInLongestSession = tracks.map(track => s"${track.artistName}->${track.trackName}").mkString("[",",","]")
      s"$userId\t$startTimestamp\t$endTimestamp\t$songsInLongestSession"
  }

  private def save(result: RDD[String]): Unit = result.saveAsTextFile("question1.tsv")
}
