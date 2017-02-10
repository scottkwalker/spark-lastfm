package spark.lastfm

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{Matchers, WordSpec}
import spark.lastfm.models.RecentTrack

trait UnitSpec extends WordSpec with Matchers {

  val userId1 = "some-user1"
  val userId2 = "some-user2"
  val userId3 = "some-user3"
  val timestamp0Minutes: LocalDateTime = LocalDateTime.parse("2009-01-01T00:00:00Z", DateTimeFormatter.ISO_DATE_TIME)
  val timestamp19Minutes = timestamp0Minutes.plusMinutes(19)
  val timestamp40Minutes = timestamp0Minutes.plusMinutes(40)
  val timestamp59Minutes = timestamp0Minutes.plusMinutes(59)
  val artistId1 = "some-artistId1"
  val artistName1 = "some-artistName1"
  val trackId1: Option[String] = None
  val trackStartOfSession = "some-trackStartOfSession"
  val trackInsideSession = "some-trackInsideSession"

  val user1TrackStartOfSession1 = RecentTrack(userId1, timestamp0Minutes, artistId1, artistName1, trackId1, trackStartOfSession)
  val user1TrackInsideSession1 = user1TrackStartOfSession1.copy(timestamp = timestamp19Minutes, trackName = trackInsideSession)
  val user1TrackStartOfSession2 = user1TrackStartOfSession1.copy(timestamp = timestamp40Minutes)
  val user1TrackInsideSession2 = user1TrackStartOfSession2.copy(timestamp = timestamp59Minutes, trackName = trackInsideSession)
  val user2TrackStartOfSession3 = user1TrackStartOfSession1.copy(userId = userId2)
  val user3TrackStartOfSession4 = user1TrackStartOfSession1.copy(userId = userId3, trackName = "some-trackName3")

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
