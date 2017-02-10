package spark.lastfm

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import spark.lastfm.models.RecentTrack

trait LastFm {

  def run(): Unit

  protected def createContext = {
    val numberOfCores = 2
    val conf = new SparkConf()
      .setAppName("spark-lastfm")
      .setMaster(s"local[$numberOfCores]")
    new SparkContext(conf)
  }

  protected def loadData(filePath: String, sc: SparkContext) = sc.textFile(filePath)

  protected def parseRecentTracks(sc: SparkContext) = {
    val recentTrackData = {
      val filePath = "userid-timestamp-artid-artname-traid-traname.tsv"
      loadData(filePath, sc)
    }

    recentTrackData.map { line =>

      def emptyStringToNone: String => Option[String] = {
        case ""       => None
        case nonEmpty => Some(nonEmpty)
      }

      // This assumes every value is present.
      val delimited = line.split("\t")
      val userId = delimited(0)
      val timestamp = LocalDateTime.parse(delimited(1), DateTimeFormatter.ISO_DATE_TIME)
      val artistId = delimited(2)
      val artistName = delimited(3)
      val trackId = emptyStringToNone(delimited(4))
      val trackName = delimited(5)
      RecentTrack(userId, timestamp, artistId, artistName, trackId, trackName)
    }
  }

  protected def tracksPlayedByUser(recentTracks: RDD[RecentTrack]) = recentTracks.groupBy(_.userId)
}