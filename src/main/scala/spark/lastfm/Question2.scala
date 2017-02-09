package spark.lastfm

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spark.lastfm.models.RecentTrack

object Question2 extends Setup {

  override def run(): Unit = {
    val sc = createContext

    def recentTracks = parseRecentTracks(sc)

    val mostPopularSongs = transform(recentTracks)
    save(mostPopularSongs, sc)

    sc.stop()
  }

  def transform(recentTracks: RDD[RecentTrack]): Array[String] =
    recentTracks.map(track => (s"${track.artistName}\t${track.trackName}", BigInt(1)))
      .reduceByKey(_ + _)
      .sortBy[BigInt](_._2, ascending = false)
      .take(100)
      .map { case (key, count) => s"$key\t$count" }

  private def save(result: Array[String], sc: SparkContext) = sc.parallelize[String](result).saveAsTextFile("question2.tsv")
}
