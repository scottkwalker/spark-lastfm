import org.apache.spark.{SparkConf, SparkContext}

object MainApp {

  def main(args: Array[String]) {
    val sc = createContext

//    val runnable = new WordCount
    val runnable = new Question1
    runnable.run(sc)

    sc.stop()
  }

  protected def createContext = {
    val numberOfCores = 2
    val conf = new SparkConf()
      .setAppName("spark-lastfm")
      .setMaster(s"local[$numberOfCores]")
    new SparkContext(conf)
  }
}

sealed trait Setup {
  def run(sc: SparkContext): Unit

  protected def loadData(filePath: String, sc: SparkContext) = sc.textFile(filePath)
}

final class WordCount extends Setup {

  def run(sc: SparkContext): Unit = {
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
}

final class Question1 extends Setup {
  def run(sc: SparkContext): Unit = {
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