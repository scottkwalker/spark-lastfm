package spark.lastfm

import spark.lastfm.models.{RecentTrack, Session}

class Question3Spec extends UnitSpec {

  "transform" should {

    "return the longest user sessions (with a limit)" in {
      withSparkContext { sc =>
        val recentTracks = List(
          user1TrackStartOfSession1, user1TrackInsideSession1,
          user1TrackStartOfSession2,
          user2TrackStartOfSession3
        )

        val stubbedContext = sc.parallelize[RecentTrack](recentTracks)

        val result = Question3.transform(stubbedContext, limit = 2)

        result should contain theSameElementsInOrderAs Array(
          // session:
          // userid, timestamp of first song in session, timestamp of last song in the session, and the list of songs played in the session (in order of play).
          s"$userId1\t$timestamp0Minutes\t$timestamp19Minutes\t[$artistName1->$trackStartOfSession,$artistName1->$trackInsideSession]",
          s"$userId2\t$timestamp0Minutes\t$timestamp0Minutes\t[$artistName1->$trackStartOfSession]"
        )
      }
    }
  }

  "tracksInsideSession" should {

    "return a single track when only one track was played" in {
      val recentTracks = List(user1TrackStartOfSession1)

      val (inSession, remainder) = Question3.tracksInsideSession(recentTracks)

      inSession shouldBe List(user1TrackStartOfSession1)
      remainder shouldBe Nil
    }

    "return tracks that are each within 20 minutes of each other and the remainder" in {
      val recentTracks = List(
        user1TrackStartOfSession1, user1TrackInsideSession1,
        user1TrackStartOfSession2
      )

      val (inSession, remainder) = Question3.tracksInsideSession(recentTracks)

      inSession shouldBe List(user1TrackStartOfSession1, user1TrackInsideSession1)
      remainder shouldBe List(user1TrackStartOfSession2)
    }
  }

  "splitIntoSessions" should {

    "return a sequence with a single session when a user has only played on track" in {
      val recentTracks = List(user1TrackStartOfSession1)

      val result = Question3.splitIntoSessions(userId1, recentTracks)

      result shouldBe List(Session(userId1, List(user1TrackStartOfSession1)))
    }

    "return multiple sessions when a user has played more than one song within a 20 minute sliding window" in {
      val recentTracks = List(user1TrackStartOfSession1, user1TrackInsideSession1, user1TrackStartOfSession2, user1TrackInsideSession2)

      val result = Question3.splitIntoSessions(userId1, recentTracks)

      result shouldBe List(
        Session(userId1, List(user1TrackStartOfSession1, user1TrackInsideSession1)),
        Session(userId1, List(user1TrackStartOfSession2, user1TrackInsideSession2))
      )
    }
  }

  "format" should {

    "return expected" in {
      val recentTracks = List(user1TrackStartOfSession1, user1TrackInsideSession1)
      val session = Session(userId1, recentTracks)

      val result = Question3.format(session)

      result shouldBe s"$userId1\t$timestamp0Minutes\t$timestamp19Minutes\t[$artistName1->$trackStartOfSession,$artistName1->$trackInsideSession]"
    }
  }
}
