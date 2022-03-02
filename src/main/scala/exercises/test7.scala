package exercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object test7 {

  val spark: SparkSession = SparkSession.builder()
    .appName("DStream Window Tranformation")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def readLines(): DStream[String] = ssc.socketTextStream("localhost", 12345)

  def frequent_contributor(): DStream[(String, Int)] = {
    ssc.checkpoint("checkpoints")

    readLines()
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKeyAndWindow(
        (a, b) => a + b, // reduction function
        (a, b) => a - b, // inverse function
        Minutes(60),
        Seconds(10)
      )
  }

  def main(args: Array[String]): Unit = {
    frequent_contributor()
    ssc.start()
    ssc.awaitTermination()
  }

}