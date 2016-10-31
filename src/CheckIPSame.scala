import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Random
import java.net.URLEncoder
import java.net.URLDecoder
import org.apache.hadoop.hdfs.server.balancer.Balancer.Matcher
import java.util.regex.Pattern
import scala.collection.immutable
import collection.immutable.HashMap

import java.sql.Timestamp
import java.util.Calendar
import scala.util.Properties
import java.util.Properties

object CheckIPSame {

  def main(args: Array[String]) {

    if (args.length != 2) {
      sys.error("Usage:<input><output>")
    }

    val Array(input, output) = args

    val sparkConf = new SparkConf().setAppName(" CheckIPSame")//.setMaster("local")

//    sparkConf.set("spark.akka.frameSize", "200")
    sparkConf.set("spark.akka.frameSize", "500")

    sparkConf.set("spark.akka.askTimeout", "600")
    sparkConf.set("spark.akka.timeout", "600")
    sparkConf.set("spark.driver.maxResultSize", "2g")

    // akka.pattern.AskTimeoutException: Timed out

    sparkConf.set("spark.shuffle.file.buffer.kb", "500")
    sparkConf.set("spark.kryoserializer.buffer.mb", "512")

    sparkConf.set("spark.core.connection.auth.wait.timeout", "300")
    sparkConf.set("spark.shuffle.consolidateFiles ", "true")
    sparkConf.set("spark.executor.memory", "2g")

    val sc = new SparkContext(sparkConf)

    var userid = ""
    var itemid = ""

    val oriRatings1 = sc.textFile(input).map(_.split(",")).filter(_.length == 4).map(fields => {

      val basenick = fields(0).split("\\(")(1)
      val linknick = fields(1)
      val timeIndex = fields(2)
      val ip = fields(3)
      val key = linknick + "," + basenick + "," + timeIndex

      //      var key = ""
      //
      //        if (basenick == "taobao_Nick|lizeng1992823") {
      //          key = linknick + "," + basenick + "," + timeIndex
      //
      //        }
      //
      //      if (linknick == "taobao_Nick|lizeng1992823") {
      //        key = linknick + "," + basenick + "," + timeIndex
      //
      //      }

      (key, ip)
    })

    //.filter(_._1 != "").saveAsTextFile(output)

    val oriRatings2 = sc.textFile(input).map(_.split(",")).filter(_.length == 4).map(fields => {

      val basenick = fields(0).split("\\(")(1)
      val linknick = fields(1)
      val timeIndex = fields(2)
      val ip = fields(3)
      val key = basenick + "," + linknick + "," + timeIndex

      //      var key = ""
      //      if (basenick == "taobao_Nick|lizeng1992823") {
      //        key = basenick + "," + linknick + "," + timeIndex
      //
      //      }
      //
      //      if (linknick == "taobao_Nick|lizeng1992823") {
      //        key = basenick + "," + linknick + "," + timeIndex
      //
      //      }

      (key, ip)
    })

    val merge = oriRatings2.union(oriRatings1).map(x => {
      val key = x._1 + "," + x._2
      (key, 1L)
    }).reduceByKey((x, y) => x + y)
    
//    val out = merge.map(x=>{
//      val key = x._1
//      val cont:Int = x._2.toInt
//      (key,cont)
//    })
    
    
    .saveAsTextFile(output)

  }

}
