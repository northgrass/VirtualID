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

object JoinRatio {

  def main(args: Array[String]) {

    if (args.length != 3) {
      sys.error("Usage:<input1><input2><output>")
    }

    val Array(input1, input2, output) = args

    val sparkConf = new SparkConf().setAppName("Join") //.setMaster("local")

    sparkConf.set("spark.akka.frameSize", "200")
    val sc = new SparkContext(sparkConf)

    var userid = ""
    var itemid = ""

    val denominatorFilter = sc.textFile(input1).map(_.split(",")).filter(_.length == 5).map(fields => {

      val nick1 = fields(0).trim()
      val nick2 = fields(1).trim()
      val info = nick1 + "," + nick2

      val key = nick1 + "," + nick2
      //      (basenick,linknick,timeIndex,1L)
      //   val baseuserflag = 1

      (key, info)

    })

    val filterUserOnlinePair = sc.textFile(input2).map(_.split(",")).filter(_.length > 7).map(fields => {

      val nick1 = fields(0).trim() + "," + fields(1).trim();
      val count = fields(2).trim() + "," + fields(4).trim() + "," + fields(6).trim();

      var key = nick1
      //      val value =
      //      (basenick,linknick,timeIndex,1L)
      //   val baseuserflag = 1

      (key, count)

    })

    
    
    val filterUserOnlinePairOppo = sc.textFile(input2).map(_.split(",")).filter(_.length > 7).map(fields => {

      val nick1 = fields(1).trim() + "," + fields(0).trim();
      val count = fields(2).trim() + "," + fields(4).trim() + "," + fields(6).trim();

      var key = nick1
      //      val value =
      //      (basenick,linknick,timeIndex,1L)
      //   val baseuserflag = 1

      (key, count)

    })
    var userPair = filterUserOnlinePair.union(filterUserOnlinePairOppo).distinct
    val joinOnlineUser = denominatorFilter.leftOuterJoin(userPair).map {

      case (key, (info, count)) => (key,count)

    }


    val out1 = joinOnlineUser.saveAsTextFile(output)
    //      val out2 = numerator.saveAsTextFile(output2)

    //    val out = index.saveAsTextFile(output1)

  }

}
