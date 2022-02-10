package exercises

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import java.io.InputStreamReader
import java.io.BufferedReader
import java.net.{URL, URLConnection}
import org.apache.spark.internal.Logging

class UrlReceiver(urlStr: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {
  override def onStart(): Unit = {
    new Thread("Url Receiver") {
      override def run(): Unit = {
        val urlConnection: URLConnection = new URL(urlStr).openConnection
        urlConnection.setRequestProperty("Accept", "application/json")
        val bufferedReader: BufferedReader = new BufferedReader(
          new InputStreamReader(urlConnection.getInputStream)
        )
        var msg = bufferedReader.readLine
        while (msg != null) {
          if (msg.nonEmpty) {
            store(msg)
          }
          msg = bufferedReader.readLine
        }
        bufferedReader.close()
      }
    }.start()
  }

  override def onStop(): Unit = {
    // nothing to do
  }
}