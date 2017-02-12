package spark.lastfm

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spark.lastfm.models.RecentTrack

object Question2 extends LastFm {

  override def run(): Unit =
    withSparkContext("question2") { sc =>

      def recentTracks = parseRecentTracks(sc)

      val mostPopularSongs = transform(recentTracks, limit = 100)

      save(mostPopularSongs, sc)
    }

  implicit val orderingKeysAndCounts: Ordering[(String, BigInt)] = new Ordering[(String, BigInt)] {
    override def compare(x: (String, BigInt), y: (String, BigInt)) = x._2.compare(y._2)
  }

  def transform(recentTracks: RDD[RecentTrack], limit: Int): Array[String] =
    recentTracks
      .map(track => (s"${track.artistName}\t${track.trackName}", BigInt(1)))
      .reduceByKey(_ + _)
      .top(limit)
      .map(format)

  def format: PartialFunction[(String, BigInt), String] = {
    case (key, count) => s"$key\t$count"
  }

  private def save(result: Array[String], sc: SparkContext) = sc.parallelize[String](result).saveAsTextFile("question2.tsv")
}
