package spark.lastfm

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}
import spark.lastfm.models.RecentTrack

class Question1Spec extends UnitSpec {

  "transform" should {

    "return user ids along with the number of distinct songs the user has played" in {
      val recentTracks = Seq(
        user1Track1, user1Track2, user1Track1, user1Track2, user1Track1, user1Track2,
        user2Track1
      )

      val stubbedContext = sc.parallelize[RecentTrack](recentTracks)

      val result = Question1.transform(stubbedContext).collect()

      result should contain theSameElementsAs Array(
        s"$userId1\t2",
        s"$userId2\t1"
      )
    }
  }

  protected var sc: SparkContext = _

  before {
    sc = {
      val master = "local[2]"
      val appName = "test-spark-lastfm-question1"
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
