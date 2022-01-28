package exercises

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object part2_g {

  val spark: SparkSession = SparkSession
    .builder
    .appName("wiki_changes")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def readFromWeb(urlString: String) {
    val webStream = ssc.receiverStream(new UrlReceiver(urlString))

    // saves webStream
    //    webStream.saveAsTextFiles("src/main/resources/results/")

    webStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readFromWeb("https://stream.wikimedia.org/v2/stream/recentchange")
  }
}








