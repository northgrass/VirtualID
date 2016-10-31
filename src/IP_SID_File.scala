import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Random
import java.net.URLEncoder
import java.net.URLDecoder
import org.apache.hadoop.hdfs.server.balancer.Balancer.Matcher
import java.util.regex.Pattern
import scala.collection.immutable
import collection.immutable.HashMap
import org.apache.spark.sql.columnar.TIMESTAMP
import java.sql.Timestamp
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.TimeZone
import java.util.Date
import java.text.ParseException

object IP_SID_File {

  def TimeStamp2Date(timestampString: Long, formats: String): String = {
    var timestamp: Long = timestampString.toLong
    var date: String = new java.text.SimpleDateFormat(formats).format(new java.util.Date(timestamp));
    date;
  }

  def main(args: Array[String]) = {

    if (args.length != 2) {
      sys.error("Usage:<input><output>")
    }

    val Array(input, output) = args

    var outputString = ""

    val conf = new SparkConf
    conf.setAppName("lvqiujian-taobao-extraction")// .setMaster("local")
    conf.set("spark.akka.frameSize", "500")

    conf.set("spark.akka.askTimeout", "600")
    conf.set("spark.akka.timeout", "600")
    
    // akka.pattern.AskTimeoutException: Timed out

    conf.set("spark.shuffle.file.buffer.kb", "500")
    conf.set("spark.kryoserializer.buffer.mb", "512")

    conf.set("spark.core.connection.auth.wait.timeout", "300")
    conf.set("spark.shuffle.consolidateFiles ", "true")

    val sc = new SparkContext(conf)
    var time = 0L
    var userip = ""
    var nick = ""

    val timeAndID = sc.textFile(input).map(_.split("\\|")).filter(_.length > 4)
      .map(x => {
        //    val y :String = URLEncoder.encode(x,"UTF8")
        time = x(0).toLong
        var key = ""
        //        time = time + 1800000
        if (x(1) == "") {
          userip = x(2)
          nick = x(3) + "|" + x(4)

          //        val timeInterval = (timeformat.toInt)*60*1000
          ////        println(time)
          //     
          //        val serialNum = time.toLong/timeInterval
          //           println(serialNum)
          //        var localtime = TimeStamp2Date(time, "yyyy-MM-dd HH:mm:00")
          //        var timeSplit = localtime.split(":")
          //        var min = timeSplit(1).toInt/(timeformat.toInt)
          //        var halfHourTime = timeSplit(0)+":"+min+":00"
          var localtime_hour = TimeStamp2Date(time, "yyyy-MM-dd HH:00:00")
          //        println(localtime_hour)
          //         println(localtime)
          //        val key = localtime_hour + "," + userip + "," + nick
          key = localtime_hour + "," + userip + "," + nick
        }
        (key, 1L)
      }).filter(_._1 != "")
      .reduceByKey((x, y) => x + y).saveAsTextFile(output)
  }
}