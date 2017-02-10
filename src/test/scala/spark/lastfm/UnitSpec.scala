package spark.lastfm

import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}
import spark.lastfm.models.RecentTrack

trait UnitSpec extends WordSpec with Matchers with BeforeAndAfter {

  val userId1 = "some-user1"
  val userId2 = "some-user2"
  val timestamp1 = "2009-05-04T23:08:57Z"
  val artistId1 = "some-artistId1"
  val artistName1 = "some-artistName1"
  val trackId1: Option[String] = None
  val trackName1 = "some-trackName1"
  val trackName2 = "some-trackName2"
  val trackName3 = "some-trackName3"

  val user1Track1 = RecentTrack(userId1, timestamp1, artistId1, artistName1, trackId1, trackName1)
  val user1Track2 = user1Track1.copy(trackName = trackName2)
  val user2Track1 = RecentTrack(userId2, timestamp1, artistId1, artistName1, trackId1, trackName1)
  val user2Track3 = user2Track1.copy(trackName = trackName3)
}
