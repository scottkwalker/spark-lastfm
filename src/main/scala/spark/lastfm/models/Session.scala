package spark.lastfm.models

import java.time.Duration

final case class Session(userId: String, tracks: List[RecentTrack]) {
  def firstSongTimestamp = tracks.head.timestamp
  def lastSongTimestamp = tracks.last.timestamp
  def duration = Duration.between(firstSongTimestamp, lastSongTimestamp)
}
