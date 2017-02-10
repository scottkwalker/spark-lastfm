package spark.lastfm

import spark.lastfm.Question3.Session
import spark.lastfm.models.RecentTrack

class Question3Spec extends UnitSpec {

  "transform" should {

    "return the longest user sessions" in {
      withSparkContext { sc =>
        val recentTracks = Seq(
          user1TrackStartOfSession1, user1TrackInsideSession1, user1TrackStartOfSession2,
          user2TrackStartOfSession1
        )

        val stubbedContext = sc.parallelize[RecentTrack](recentTracks)

        val result = Question3.transform(stubbedContext, limit = 2).collect()

        result should contain theSameElementsAs Array(
          // session:
          // userid, timestamp of first song in session, timestamp of last song in the session, and the list of songs played in the session (in order of play).
          s"$userId1\t$timestampStartOfSession1\t$timestampInsideSession1\t[$artistName1->$trackName1,$artistName1->$trackName2]",
          s"$userId2\t$timestampStartOfSession1\t$timestampStartOfSession1\t[$artistName1->$trackName1]"
        )
      }
    }
  }

  "splitIntoSessions" should {

    "return a sequence with a single session when a user has only played on track" in {
      val recentTracks = Seq(user1TrackStartOfSession1)

      val result = Question3.splitIntoSessions((userId1, recentTracks))

      //List(Session(some-user1,List(RecentTrack(some-user1,2009-01-01T00:00,some-artistId1,some-artistName1,None,some-trackName1))))
      //Session(some-user1,List(RecentTrack(some-user1,2009-01-01T00:00,some-artistId1,some-artistName1,None,some-trackName1)))

      result shouldBe Iterable(Session(userId1, Seq(user1TrackStartOfSession1)))
    }

    "return multiple sessions when a user has played more than one song within a 20 minute sliding window" in {
      val recentTracks = Seq(user1TrackStartOfSession1, user1TrackInsideSession1, user1TrackStartOfSession2, user1TrackInsideSession2)

      val result = Question3.splitIntoSessions((userId1, recentTracks))

      result shouldBe Iterable(
        Session(userId1, Seq(user1TrackStartOfSession1, user1TrackInsideSession1)),
        Session(userId1, Seq(user1TrackStartOfSession2, user1TrackInsideSession2))
      )
    }
  }

  "format" should {

    "return expected" in {
      val recentTracks = Seq(user1TrackStartOfSession1, user1TrackInsideSession1)

      val result = Question3.format(Session(userId1, recentTracks))

      result shouldBe s"$userId1\t$timestampStartOfSession1\t$timestampInsideSession1\t[$artistName1->$trackName1,$artistName1->$trackName2]"
    }
  }
}
