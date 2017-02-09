package spark.lastfm

import org.apache.spark.{SparkConf, SparkContext}

trait Setup {

  protected def createContext = {
    val numberOfCores = 2
    val conf = new SparkConf()
      .setAppName("spark-lastfm")
      .setMaster(s"local[$numberOfCores]")
    new SparkContext(conf)
  }

  protected def loadData(filePath: String, sc: SparkContext) = sc.textFile(filePath)
}