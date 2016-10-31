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

object RemainUserJoinPair {

  def main(args: Array[String]) {

    if (args.length != 3) {
      sys.error("Usage:<input1><input2><output>")
    }

    val Array(input1, input2, output) = args

    val sparkConf = new SparkConf().setAppName(" RemainUserIP Match") //.setMaster("local")

    sparkConf.set("spark.akka.frameSize", "100")
    val sc = new SparkContext(sparkConf)

    var userid = ""
    var itemid = ""

    val oriRatings = sc.textFile(input1).map(_.split(",")).filter(_.length > 2).map(fields => {

      val usernick1 = fields(0)
      val usernick2 = fields(1)

      (usernick1, usernick2)
    })

    val oriRatingsOppo = sc.textFile(input1).map(_.split(",")).filter(_.length > 2).map(fields => {

      val usernick1 = fields(0)
      val usernick2 = fields(1)

      (usernick2, usernick1)
    })

    var userPair = oriRatings.union(oriRatingsOppo).distinct

    val oriRatings1 = sc.textFile(input2).map(_.split(",")).filter(_.length == 3).map(fields => {

      var usernick = fields(0).split("\\(")(1)
      val timeIndex = fields(1)
      val ip = fields(2)
      var info = timeIndex + "," + ip

//      if ((usernick.contains("taobao_Nick|lizeng1992823")) || (usernick.contains("QQ_normal|121180294"))) {
//
//      } else {
//        usernick = ""
//        info = ""
//
//      }

      (usernick, info)
    })

    val merge = userPair.join(oriRatings1).map {
      case (id, (usernick, info)) => (id, usernick, info)
    }

//    val test = merge.map(x => {
//      var id = ""
//      var usernick = ""
//      var info = ""
//      if ((x._1.contains("taobao_Nick|lizeng1992823")) || (x._1.contains("QQ_normal|121180294"))) {
//        id = x._1
//        usernick = x._2
//        info = x._3
//      } else if ((x._2.contains("taobao_Nick|lizeng1992823")) || (x._2.contains("QQ_normal|121180294"))) {
//        id = x._1
//        usernick = x._2
//        info = x._3
//      } else {
//        id = ""
//        usernick = ""
//        info = ""
//
//      }
//
//      val output = id + "," + usernick + "," + info
//      output
//
//    })

//    val out = oriRatings1.saveAsTextFile(output)
    val out = merge.saveAsTextFile(output)

  }

}
