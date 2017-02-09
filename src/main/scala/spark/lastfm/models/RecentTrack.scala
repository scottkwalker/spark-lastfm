package spark.lastfm.models

final case class RecentTrack(userId: String, timestamp: String, artistId: String, artistName: String, trackId: Option[String], trackName: String)