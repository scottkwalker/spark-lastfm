package spark.lastfm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

class Question1Spec extends WordSpec with Matchers with BeforeAndAfter {

  "transform" should {

    "return without duplicate tracks" in {
      val userId1 = "some-user1"
      val timestamp1 = "2009-05-04T23:08:57Z"
      val artistId1 = "some-artistId1"
      val artistName1 = "some-artistName1"
      val trackId1: Option[String] = None
      val trackName1 = "some-trackName1"
      val trackName2 = "some-trackName2"
      val user1Track1 = RecentTrack(userId1, timestamp1, artistId1, artistName1, trackId1, trackName1)
      val user1Track2 = user1Track1.copy(trackName = trackName2)
      val recentTracks = Seq(user1Track1, user1Track2, user1Track1, user1Track2, user1Track1, user1Track2)

      val stubbedContext = sc.parallelize[RecentTrack](recentTracks)
      val result = Question1.transform(stubbedContext).collect

      result should equal(Array(
        s"$userId1\t2"
      ))
    }
  }

  private var sc: SparkContext = _

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
