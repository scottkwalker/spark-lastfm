package spark.lastfm

import spark.lastfm.models.RecentTrack

class Question2Spec extends UnitSpec {

  "transform" should {

    "return tracks that have the highest number of times played (with a limit)" in {
      withSparkContext { sc =>
        val recentTracks = Seq(
          user1TrackInsideSession1, user1TrackInsideSession1,
          user1TrackStartOfSession1, user1TrackStartOfSession1, user1TrackStartOfSession1, user2TrackStartOfSession3,
          user3TrackStartOfSession4
        )

        val stubbedContext = sc.parallelize[RecentTrack](recentTracks)

        val result = Question2.transform(stubbedContext, limit = 2)

        result should contain theSameElementsAs Array(
          s"$artistName1\t$trackStartOfSession\t4",
          s"$artistName1\t$trackInsideSession\t2"
        )
      }
    }
  }

  "format" should {

    "return the user id and count in tab separated" in {
      Question2.format("some-key", 2) shouldBe "some-key\t2"
    }
  }
}