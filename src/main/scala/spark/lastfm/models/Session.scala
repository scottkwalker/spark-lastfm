package spark.lastfm.models

final case class Session(userId: String, tracks: List[RecentTrack]) {
  def startTimestamp = tracks.head.timestamp
  def endTimestamp = tracks.last.timestamp
}
