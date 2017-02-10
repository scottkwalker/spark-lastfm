package spark.lastfm

import spark.lastfm.models.RecentTrack

class Question3Spec extends UnitSpec {

  "transform" should {

    "return the longest user sessions" in {
      withSparkContext { sc =>
        val recentTracks = Seq(
          user1TrackStartOfSession, user1TrackInSession, user1TrackOutsideSession,
          user2TrackStartOfSession
        )

        val stubbedContext = sc.parallelize[RecentTrack](recentTracks)

        val result = Question3.transform(stubbedContext, limit = 2).collect()

        result should contain theSameElementsAs Array(
          // session:
          // userid, timestamp of first song in session, timestamp of last song in the session, and the list of songs played in the session (in order of play).
          s"$userId1\t$timestampStartOfSession\t$timestampInSession\t[$artistName1->$trackName1,$artistName1->$trackName2]",
          s"$userId2\t$timestampStartOfSession\t$timestampStartOfSession\t[$artistName1->$trackName1]"
        )
      }
    }
  }

  "format" should {

    "return expected" in {
      val recentTracks = Seq(user1TrackStartOfSession, user1TrackInSession)

      val result = Question3.format((userId1, recentTracks))

      result shouldBe s"$userId1\t$timestampStartOfSession\t$timestampInSession\t[$artistName1->$trackName1,$artistName1->$trackName2]"
    }
  }
}
