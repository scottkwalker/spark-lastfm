import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {

  def main(args: Array[String]) {
    val sc = createContext
    val filePath = "README.md"
    val data = loadData(filePath, sc)

    wordCount(data)

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

  private def wordCount(data: RDD[String]) = {
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
