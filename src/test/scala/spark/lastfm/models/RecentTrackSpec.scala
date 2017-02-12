package spark.lastfm.models

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import spark.lastfm.UnitSpec

class RecentTrackSpec extends UnitSpec {

  "endOfSession" should {

    "return a time 20 minutes into the future" in {
      user1TrackStartOfSession1.endOfSession shouldBe LocalDateTime.parse("2009-01-01T00:20:00Z", DateTimeFormatter.ISO_DATE_TIME)
    }
  }
}
