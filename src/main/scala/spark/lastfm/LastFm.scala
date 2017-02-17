package spark.lastfm

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import spark.lastfm.models.RecentTrack

trait LastFm {

  def run(): Unit

  protected def withSparkContext(taskName: String)(body: SparkContext => Unit): Unit = {
    val sc = {
      val numberOfCores = Runtime.getRuntime.availableProcessors()
      val conf = new SparkConf()
        .setAppName(s"spark-lastfm-$taskName")
        .setMaster(s"local[$numberOfCores]")
      new SparkContext(conf)
    }

    try {
      body(sc)
    } finally {
      sc.stop()
    }
  }

  protected def parseRecentTracks(sc: SparkContext) = {

    def readFromFile(sc: SparkContext) = {
      val filePath = "userid-timestamp-artid-artname-traid-traname.tsv"
      sc.textFile(filePath)
    }

    def parse(tabDelimitedTokens: String) = {

      def optionalToken: String => Option[String] = {
        case ""       => None
        case nonEmpty => Some(nonEmpty)
      }

      def toTimestamp(token: String) = LocalDateTime.parse(token, DateTimeFormatter.ISO_DATE_TIME)

      // This assumes every value is present.
      val tokens = tabDelimitedTokens.split("\t")
      val userId = tokens(0)
      val timestamp = toTimestamp(tokens(1))
      val artistId = tokens(2)
      val artistName = tokens(3)
      val trackId = optionalToken(tokens(4))
      val trackName = tokens(5)
      RecentTrack(userId, timestamp, artistId, artistName, trackId, trackName)
    }

    readFromFile(sc).map(parse)
  }

  protected def tracksPlayedByUser(recentTracks: RDD[RecentTrack]) = recentTracks.groupBy(_.userId)
}