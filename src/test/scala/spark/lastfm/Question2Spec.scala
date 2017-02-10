package spark.lastfm

import spark.lastfm.models.RecentTrack

class Question2Spec extends UnitSpec {

  "transform" should {

    "return tracks that have the highest number of times played (with a limit)" in {
      withSparkContext { sc =>
        val recentTracks = Seq(
          user1TrackInSession, user1TrackInSession,
          user1TrackStartOfSession, user1TrackStartOfSession, user1TrackStartOfSession, user2TrackStartOfSession,
          user2Track3
        )

        val stubbedContext = sc.parallelize[RecentTrack](recentTracks)

        val result = Question2.transform(stubbedContext, limit = 2)

        result should contain theSameElementsAs Array(
          s"$artistName1\t$trackName1\t4",
          s"$artistName1\t$trackName2\t2"
        )
      }
    }
  }
}