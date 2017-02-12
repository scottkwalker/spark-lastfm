package spark.lastfm.models

import java.time.LocalDateTime

final case class RecentTrack(userId: String, timestamp: LocalDateTime, artistId: String, artistName: String, trackId: Option[String], trackName: String) {
  def endOfSession = timestamp.plusMinutes(20)
}