package spark.lastfm

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{Matchers, WordSpec}
import spark.lastfm.models.RecentTrack

trait UnitSpec extends WordSpec with Matchers {

  val userId1 = "some-user1"
  val userId2 = "some-user2"
  val timestampStartOfSession1: LocalDateTime = LocalDateTime.parse("2009-01-01T00:00:00Z", DateTimeFormatter.ISO_DATE_TIME)
  val timestampInsideSession1 = timestampStartOfSession1.plusMinutes(19)
  val timestampStartOfSession2 = timestampStartOfSession1.plusMinutes(20)
  val timestampInsideSession2 = timestampStartOfSession1.plusMinutes(39)
  val artistId1 = "some-artistId1"
  val artistName1 = "some-artistName1"
  val trackId1: Option[String] = None
  val trackName1 = "some-trackName1"
  val trackName2 = "some-trackName2"

  val user1TrackStartOfSession1 = RecentTrack(userId1, timestampStartOfSession1, artistId1, artistName1, trackId1, trackName1)
  val user1TrackInsideSession1 = user1TrackStartOfSession1.copy(timestamp = timestampInsideSession1, trackName = trackName2)
  val user1TrackStartOfSession2 = user1TrackStartOfSession1.copy(timestamp = timestampStartOfSession2, trackName = trackName2)
  val user1TrackInsideSession2 = user1TrackStartOfSession1.copy(timestamp = timestampInsideSession2, trackName = trackName2)
  val user2TrackStartOfSession1 = user1TrackStartOfSession1.copy(userId = userId2)

  protected def withSparkContext(body: SparkContext => Unit): Unit = {
    val sc = {
      val master = "local[2]"
      val appName = "test-spark-lastfm-question1"
      val conf = new SparkConf()
        .setMaster(master)
        .setAppName(appName)

      new SparkContext(conf)
    }

    try {
      body(sc)
    } finally {
      sc.stop()
    }
  }
}
