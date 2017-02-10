package spark.lastfm

import spark.lastfm.models.RecentTrack

class Question1Spec extends UnitSpec {

  "transform" should {

    "return user ids along with the number of distinct songs the user has played" in {
      withSparkContext { sc =>
        val recentTracks = Seq(
          user1TrackStartOfSession, user1TrackInSession, user1TrackStartOfSession, user1TrackInSession, user1TrackStartOfSession, user1TrackInSession,
          user2TrackStartOfSession
        )

        val stubbedContext = sc.parallelize[RecentTrack](recentTracks)

        val result = Question1.transform(stubbedContext).collect()

        result should contain theSameElementsAs Array(
          s"$userId1\t2",
          s"$userId2\t1"
        )
      }
    }
  }

  "countDistinctTracksForUser" should {

    "return user id and the count of distinct tracks" in {
      val recentTracks = Seq(user1TrackStartOfSession, user1TrackInSession, user1TrackStartOfSession, user1TrackInSession)

      val result = Question1.countDistinctTracksForUser((userId1, recentTracks))

      result shouldBe s"$userId1\t2"
    }
  }
}
