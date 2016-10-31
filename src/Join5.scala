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

object Join5 {

  def main(args: Array[String]) {

    if (args.length != 3) {
      sys.error("Usage:<input1><input2><output>")
    }

    val Array(input1, input2, output) = args

    val sparkConf = new SparkConf().setAppName("Join") //.setMaster("local")

    sparkConf.set("spark.akka.frameSize", "100")
    val sc = new SparkContext(sparkConf)

    var userid = ""
    var itemid = ""

    val denominatorFilter = sc.textFile(input1).map(_.split(",")).filter(_.length == 15).map(fields => {

      val nick1 = fields(0).trim()
      val info = fields(1).trim() + "," + fields(2).trim() + "," + fields(3).trim() + "," + fields(4).trim() + "," + fields(5).trim() + "," + fields(6).trim() + "," + fields(7).trim() + "," + fields(8).trim()  + "," + fields(9).trim() + "," + fields(10).trim() + "," + fields(11).trim() + "," + fields(12).trim()  + "," + fields(13).trim() + "," + fields(14).trim()

      val key = nick1
      //      (basenick,linknick,timeIndex,1L)
      //   val baseuserflag = 1

      (key, info)

    })

    val filterUserOnlinePair = sc.textFile(input2).map(_.split(",List\\(")).filter(_.length == 2).map(fields => {

      val nick1 = fields(0).replace("(", "")
      val count = fields(1).replace(",", "|").replace("))", "")

      var key = nick1
      //      val value =
      //      (basenick,linknick,timeIndex,1L)
      //   val baseuserflag = 1

      (key, count)

    })

    val joinOnlineUser = denominatorFilter.leftOuterJoin(filterUserOnlinePair).map {

      case (key, (info, count)) => (key,info, count)

    }


    val out1 = joinOnlineUser.saveAsTextFile(output)
    //      val out2 = numerator.saveAsTextFile(output2)

    //    val out = index.saveAsTextFile(output1)

  }

}
