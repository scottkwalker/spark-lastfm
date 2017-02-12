package spark.lastfm

import java.time.{Duration, LocalDateTime}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spark.lastfm.models.RecentTrack

import scala.annotation.tailrec

object Question3 extends LastFm {

  final case class Session(userId: String, startTimestamp: LocalDateTime, endTimestamp: LocalDateTime, tracks: Iterable[RecentTrack])

  override def run(): Unit = {
    val sc = createContext

    def recentTracks = parseRecentTracks(sc)

    val countDistinctTracksForUsers = transform(recentTracks, limit = 10)
    save(countDistinctTracksForUsers, sc)

    sc.stop()
  }

  def transform(recentTracks: RDD[RecentTrack], limit: Int) = {
    tracksPlayedByUser(recentTracks)
      .flatMap { case (userId, tracks) => splitIntoSessions(userId, tracks.toList) }
      .sortBy(f => Duration.between(f.startTimestamp, f.endTimestamp), ascending = false)
      .take(limit)
      .map(format)
  }

  def takeOneSession(tracks: List[RecentTrack]) = {

    @tailrec
    def takeOneSession(tracks: List[RecentTrack], accumulator: List[RecentTrack]): (List[RecentTrack], List[RecentTrack]) =
      tracks match {
        case head :: Nil       => (accumulator, List.empty[RecentTrack])
        case head :: remainder =>
          remainder.headOption match {
            case None       => (accumulator, List.empty[RecentTrack])
            case Some(next) =>
              val endOfSession = head.timestamp.plusMinutes(20)
              if (next.timestamp.isAfter(endOfSession)) (accumulator, remainder)
              else takeOneSession(remainder, accumulator ++ List(next))
          }
      }

    takeOneSession(tracks, accumulator = List(tracks.head))
  }

  def splitIntoSessions(userId: String, tracks: List[RecentTrack]) = {

    @tailrec
    def splitIntoSessions(tracks: List[RecentTrack], accumulator: List[Session]): List[Session] = tracks match {
      case Nil => accumulator
      case _   =>
        val (inSession, remainder) = takeOneSession(tracks)
        val startTimestamp = inSession.head.timestamp
        val endTimestamp = inSession.last.timestamp
        val session = Session(userId, startTimestamp, endTimestamp, inSession)
        splitIntoSessions(remainder, accumulator ++ List(session))
    }

    splitIntoSessions(tracks, accumulator = List.empty[Session])
  }

  def format: PartialFunction[Session, String] = {
    case Session(userId, startTimestamp, endTimestamp, tracks) =>
      val songsInLongestSession = tracks.map(track => s"${track.artistName}->${track.trackName}").mkString("[", ",", "]")
      s"$userId\t$startTimestamp\t$endTimestamp\t$songsInLongestSession"
  }

  private def save(result: Array[String], sc: SparkContext) = sc.parallelize[String](result).saveAsTextFile("question3.tsv")
}
