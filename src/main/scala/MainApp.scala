import org.apache.spark.{SparkConf, SparkContext}

object Question1App extends Setup {

  final case class User(id: String)

  final case class RecentTrack(userId: String, timestamp: String, artistId: String, artistName: String, trackId: Option[String], trackName: String)

  def main(args: Array[String]) {
    val sc = createContext

    //val users = parseUsers(sc)
    def recentTracks = parseRecentTracks(sc)

    def tracksPlayedByUser = recentTracks.groupBy(_.userId)

    // TODO could this be turned into a for-comprehension
    def countDistinctTracksForUsers = tracksPlayedByUser.map {
      case (userId, tracks) => s"$userId\t${tracks.toSeq.distinct.size}"
    }

    countDistinctTracksForUsers.saveAsTextFile("question1.tsv")

    sc.stop()
  }

  private def parseUsers(sc: SparkContext) = {
    val userData = {
      val filePath = "userid-profile.tsv"
      loadData(filePath, sc)
    }

    userData.filter(_.startsWith("#id")).map { line =>
      // This is inefficient as we only need the first value but we split the whole string.
      val delimited = line.split("\t")
      val id = delimited.head
      User(id)
    }
  }

  private def parseRecentTracks(sc: SparkContext) = {
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
      val timestamp = delimited(1)
      val artistId = delimited(2)
      val artistName = delimited(3)
      val trackId = emptyStringToNone(delimited(4))
      val trackName = delimited(5)
      RecentTrack(userId, timestamp, artistId, artistName, trackId, trackName)
    }
  }
}

sealed trait Setup {

  protected def createContext = {
    val numberOfCores = 2
    val conf = new SparkConf()
      .setAppName("spark-lastfm")
      .setMaster(s"local[$numberOfCores]")
    new SparkContext(conf)
  }

  protected def loadData(filePath: String, sc: SparkContext) = sc.textFile(filePath)
}