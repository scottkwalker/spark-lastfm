import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {

  def main(args: Array[String]) {
    val sc = createContext

    //wordCount(sc)
    question1(sc)

    sc.stop()
  }

  private def createContext = {
    val numberOfCores = 2
    val conf = new SparkConf()
      .setAppName("spark-lastfm")
      .setMaster(s"local[$numberOfCores]")
    new SparkContext(conf)
  }

  private def loadData(filePath: String, sc: SparkContext) = sc.textFile(filePath)

  private def wordCount(sc: SparkContext): Unit = {
    val filePath = "README.md"
    val data = loadData(filePath, sc)
    // Split up into words.
    val words = data.flatMap(line => line.split(" "))
    // Transform into word and count.
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    // Save the word count back out to a text file, causing evaluation.
    //counts.saveAsTextFile(outputFile)
    counts.foreach {
      case (word, count) => println(s"word: $word, count: $count")
    }
  }

  private def question1(sc: SparkContext): Unit = {
    case class User(id: String)

    val filePath = "userid-profile.tsv"
    val data = loadData(filePath, sc)

    val users = data.map { line =>
      val delimited = line.split("\t")
      // This is inefficient as we only need the first value but we split the whole string.
      val id = delimited.head
      User(id)
    }

    users.foreach(user => println(s"user id: ${user.id}"))
  }
}
