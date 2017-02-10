package spark.lastfm

import spark.lastfm.models.RecentTrack

class Question2Spec extends UnitSpec {

  "transform" should {

    "return tracks that have the highest number of times played (with a limit)" in {
      withSparkContext { sc =>
        val outsideLimit = user2TrackStartOfSession1.copy(trackName = "some-trackName3")
        val recentTracks = Seq(
          user1TrackInsideSession1, user1TrackInsideSession1,
          user1TrackStartOfSession1, user1TrackStartOfSession1, user1TrackStartOfSession1, user2TrackStartOfSession1,
          outsideLimit
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