package spark.lastfm

import java.time.{Duration, LocalDateTime}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spark.lastfm.models.{RecentTrack, Session}

import scala.annotation.tailrec

object Question3 extends LastFm {

  override def run(): Unit = {
    withSparkContext("question3") { sc =>

      def recentTracks = parseRecentTracks(sc)

      val countDistinctTracksForUsers = transform(recentTracks, limit = 10)

      save(countDistinctTracksForUsers, sc)
    }
  }

  def transform(recentTracks: RDD[RecentTrack], limit: Int) = {
    tracksPlayedByUser(recentTracks)
      .flatMap { case (userId, tracks) => splitIntoSessions(userId, tracks.toList) }
      .sortBy(f => Duration.between(f.startTimestamp, f.endTimestamp), ascending = false)
      .take(limit)
      .map(format)
  }

  def tracksInsideSession(tracks: List[RecentTrack]) = {

    @tailrec
    def tracksInFirstSession(endOfSession: LocalDateTime, tracks: List[RecentTrack], tracksInSession: List[RecentTrack]): (List[RecentTrack], List[RecentTrack]) =
      tracks match {
        case Nil                      => (tracksInSession, List.empty[RecentTrack]) // No more tracks.
        case nextTrack :: laterTracks =>
          if (nextTrack.timestamp.isAfter(endOfSession)) (tracksInSession, tracks) // The next track was not in the session.
          else tracksInFirstSession(nextTrack.endOfSession, laterTracks, tracksInSession ++ List(nextTrack))
      }

    tracksInFirstSession(tracks.head.endOfSession, tracks.tail, tracksInSession = List(tracks.head))
  }

  def splitIntoSessions(userId: String, tracks: List[RecentTrack]) = {

    @tailrec
    def splitIntoSessions(tracks: List[RecentTrack], sessions: List[Session]): List[Session] = tracks match {
      case Nil => sessions
      case _   =>
        val (tracksInSession, tracksOutsideSession) = tracksInsideSession(tracks)
        val session = Session(userId, tracksInSession)
        splitIntoSessions(tracksOutsideSession, sessions ++ List(session))
    }

    splitIntoSessions(tracks, sessions = List.empty[Session])
  }

  def format: PartialFunction[Session, String] = {
    case session: Session =>
      val songsInLongestSession = session.tracks.map(track => s"${track.artistName}->${track.trackName}").mkString("[", ",", "]")
      s"${session.userId}\t${session.startTimestamp}\t${session.endTimestamp}\t$songsInLongestSession"
  }

  private def save(result: Array[String], sc: SparkContext) = sc.parallelize[String](result).saveAsTextFile("question3.tsv")
}
