package spark.lastfm

import org.apache.spark.rdd.RDD
import spark.lastfm.models.RecentTrack

object Question3 extends LastFm {

  final case class Session(userId: String, tracks: Iterable[RecentTrack])

  override def run(): Unit = {
    val sc = createContext

    def recentTracks = parseRecentTracks(sc)

    val countDistinctTracksForUsers = transform(recentTracks, limit = 10)
    save(countDistinctTracksForUsers)

    sc.stop()
  }

  def transform(recentTracks: RDD[RecentTrack], limit: Int) = {
    tracksPlayedByUser(recentTracks).flatMap(splitIntoSessions).map(format)


    // TODO find longest sessions.
    // TODO use limit
  }

  def splitIntoSessions: PartialFunction[(String, Iterable[RecentTrack]), Iterable[Session]] = {
    case (userId, tracks) =>
      val startOfSession = tracks.head.timestamp
      val endOfSession = startOfSession.plusMinutes(20)
      val (inSession, outsideSession) = tracks.span(_.timestamp.isBefore(endOfSession))
      // TODO recurse passing outsideSession
      Seq(Session(userId, inSession))
  }

  def format: PartialFunction[Session, String] = {
    case Session(userId, tracks) =>
      val sessionFirst = tracks.head
      val sessionLast = tracks.last
      val startTimestamp = sessionFirst.timestamp
      val endTimestamp = sessionLast.timestamp
      val songsInLongestSession = tracks.map(track => s"${track.artistName}->${track.trackName}").mkString("[",",","]")
      s"$userId\t$startTimestamp\t$endTimestamp\t$songsInLongestSession"
  }

  private def save(result: RDD[String]): Unit = result.saveAsTextFile("question1.tsv")
}
