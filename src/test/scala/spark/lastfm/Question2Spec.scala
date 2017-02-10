package spark.lastfm

import org.apache.spark.{SparkConf, SparkContext}
import spark.lastfm.models.RecentTrack

class Question2Spec extends UnitSpec {

  "transform" should {

    "return tracks that have the highest number of times played (with a limit)" in {
      val recentTracks = Seq(
        user1Track2, user1Track2,
        user1Track1, user1Track1, user1Track1, user2Track1,
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

  protected var sc: SparkContext = _

  before {
    sc = {
      val master = "local[2]"
      val appName = "test-spark-lastfm-question2"
      val conf = new SparkConf()
        .setMaster(master)
        .setAppName(appName)

      new SparkContext(conf)
    }
  }

  after {
    sc.stop()
  }
}