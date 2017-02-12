package spark.lastfm.models

import spark.lastfm.UnitSpec

class SessionSpec extends UnitSpec {

  "firstSongTimestamp" should {

    "return timestamp of the first track" in {
      Session(userId1, List(user1TrackStartOfSession1, user1TrackInsideSession1)).firstSongTimestamp shouldBe timestamp0Minutes
    }
  }

  "lastSongTimestamp" should {

    "return timestamp of the last track" in {
      Session(userId1, List(user1TrackStartOfSession1, user1TrackInsideSession1)).lastSongTimestamp shouldBe timestamp19Minutes
    }
  }

  "duration" should {

    "return the difference between the fist and last song" in {
      Session(userId1, List(user1TrackStartOfSession1, user1TrackInsideSession1)).duration shouldBe java.time.Duration.ofMinutes(19)
    }
  }

}
