package spark.lastfm.models

import java.time.LocalDateTime

final case class Session(userId: String, startTimestamp: LocalDateTime, endTimestamp: LocalDateTime, tracks: List[RecentTrack])
