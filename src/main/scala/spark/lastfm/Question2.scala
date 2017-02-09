package spark.lastfm

import org.apache.spark.rdd.RDD

object Question2 extends Setup {
  override def run(): Unit = {
    val sc = createContext

    def recentTracks = parseRecentTracks(sc)
    val mostPopularSongs = transform(recentTracks)
    save(mostPopularSongs)

    sc.stop()
  }

  override protected def save(result: RDD[String]) = result.saveAsTextFile("question1.tsv")

  def transform(recentTracks: RDD[RecentTrack]): RDD[String] = {
    // list of the 100 most popular songs (artist and title)
    // with the number of times  each was played.
    ???
  }
}
