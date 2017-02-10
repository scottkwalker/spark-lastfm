package spark.lastfm

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{Matchers, WordSpec}
import spark.lastfm.models.RecentTrack

trait UnitSpec extends WordSpec with Matchers {

  val userId1 = "some-user1"
  val userId2 = "some-user2"
  val timestampStartOfSession: LocalDateTime = LocalDateTime.parse("2009-01-01T00:00:00Z", DateTimeFormatter.ISO_DATE_TIME)
  val timestampInSession = timestampStartOfSession.plusMinutes(20)
  val timestampOutsideSession = timestampStartOfSession.plusMinutes(60)
  val artistId1 = "some-artistId1"
  val artistName1 = "some-artistName1"
  val trackId1: Option[String] = None
  val trackName1 = "some-trackName1"
  val trackName2 = "some-trackName2"
  val trackName3 = "some-trackName3"

  val user1TrackStartOfSession = RecentTrack(userId1, timestampStartOfSession, artistId1, artistName1, trackId1, trackName1)
  val user1TrackInSession = user1TrackStartOfSession.copy(timestamp = timestampInSession, trackName = trackName2)
  val user1TrackOutsideSession = user1TrackStartOfSession.copy(timestamp = timestampOutsideSession, trackName = trackName2)
  val user2TrackStartOfSession = RecentTrack(userId2, timestampStartOfSession, artistId1, artistName1, trackId1, trackName1)
  val user2Track3 = user2TrackStartOfSession.copy(trackName = trackName3)


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
