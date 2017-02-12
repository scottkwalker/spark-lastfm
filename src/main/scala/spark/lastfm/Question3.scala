package spark.lastfm

import java.time.Duration

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

  def takeOneSession(tracks: List[RecentTrack]) = {

    @tailrec
    def takeOneSession(tracks: List[RecentTrack], accumulator: List[RecentTrack]): (List[RecentTrack], List[RecentTrack]) =
      tracks match {
        case Nil | _ :: Nil       => (accumulator, List.empty[RecentTrack])
        case head :: remainder =>
          remainder.headOption match {
            case None       => (accumulator, List.empty[RecentTrack])
            case Some(next) =>
              val endOfSession = head.endOfSession
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
