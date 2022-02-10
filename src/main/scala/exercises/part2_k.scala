package exercises

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spray.json._
import DefaultJsonProtocol._
import org.apache.spark.sql.functions.{col, from_unixtime, window}

object part2_k {

  val spark: SparkSession = SparkSession
    .builder
    .appName("wiki_changes_k")
    .master("local[4]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  case class Length (old: Option[Int], _new: Option[Int])
  case class WikiEvent(title: String, user: String, timestamp: Int, length: Option[Length])

  implicit object LengthFormat extends RootJsonFormat[Length] {
    override def write(l: Length): JsValue = JsObject(
      "old" -> l.old.toJson,
      "new" -> l._new.toJson
    )
    def read(json: JsValue): Length = {
      val jsObject = json.asJsObject
      jsObject.getFields() match {
        case Seq() ⇒ Length(
          jsObject.fields.get("old").map(_.convertTo[Int]),
          jsObject.fields.get("new").map(_.convertTo[Int])
        )
        case other ⇒ deserializationError("Cannot deserialize ProductItem: invalid input. Raw input: " + other)
      }
    }
  }

  implicit object wikiFormat extends RootJsonFormat[WikiEvent] {
    def write(item: WikiEvent): JsObject = JsObject(
      "title" -> item.title.toJson,
      "user" -> item.user.toJson,
      "timestamp" -> item.timestamp.toJson,
      "length" -> item.length.toJson
    )
    def read(json: JsValue): WikiEvent = {
      val jsObject = json.asJsObject

      jsObject.getFields("title", "user", "timestamp") match {
        case Seq(title, user, timestamp) ⇒ WikiEvent(
          title.convertTo[String],
          user.convertTo[String],
          timestamp.convertTo[Int],
          jsObject.fields.get("length").map(_.convertTo[Length]),
        )
        case other ⇒ deserializationError("Cannot deserialize ProductItem: invalid input. Raw input: " + other)
      }
    }
  }

  def readFromWeb(urlString: String) {
    import spark.implicits._
    val webStream = ssc.receiverStream(new UrlReceiver(urlString))

    val webStream_json = webStream.map(_.parseJson.convertTo[WikiEvent])

    webStream_json.foreachRDD( rdd => {
      val results_test =
        rdd.toDF().select(
          col("title"),
          col("user"),
          from_unixtime(col("timestamp")).as("time"),
          col("length.old").as("old size"),
          col("length._new").as("new size"))
          .withColumn("size difference",col("new size") - col("old size"))

      results_test.show()

    })

   // webStream_json.print()

    ssc.start()
    ssc.awaitTermination
  }

  def main(args: Array[String]): Unit = {
    readFromWeb("https://stream.wikimedia.org/v2/stream/recentchange")
  }
}







