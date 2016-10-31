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

object OnlineTimeCheck {

  def main(args: Array[String]) {

    if (args.length != 4) {
      sys.error("Usage:<input1><input2><output1><output2>")
    }

    val Array(input1, input2, output1, output2) = args

    val sparkConf = new SparkConf().setAppName("Online Time same IP Check")//.setMaster("local")

    sparkConf.set("spark.akka.frameSize", "500")
    val sc = new SparkContext(sparkConf)

    var userid = ""
    var itemid = ""

    val denominatorFilter = sc.textFile(input1).map(_.split(",")).filter(_.length == 5).map(fields => {

      val nick1 = fields(0).split("\\(")(1)
      val nick2 = fields(1)
      val timeIndex = fields(2)

      val key = nick1 + "," + nick2 + "," + timeIndex
      //      (basenick,linknick,timeIndex,1L)
      //   val baseuserflag = 1

      (key, 1L)

    })

    val filterUserOnlinePair = sc.textFile(input2).map(_.split(",")).filter(_.length == 5).map(fields => {

      val nick1 = fields(0).split("\\(")(1)
      val nick2 = fields(1)
      val timeIndex = fields(2)
      val ip = fields(3)
      val count = fields(4).split("\\)")(0)

      var key = nick1 + "," + nick2 + "," + timeIndex
      //      val value =
      //      (basenick,linknick,timeIndex,1L)
      //   val baseuserflag = 1

      (key, count)

    }).filter(_._2 == "2").distinct

    val joinOnlineUser = denominatorFilter.join(filterUserOnlinePair).map {

      case (key, (int, count)) => (key, count)

    }

    val numerator = joinOnlineUser.map(x => {

      val key = x._1
      val newkey = key.split(",")(0) + "," + key.split(",")(1)

      (newkey, 1L)

    }).reduceByKey((x, y) => x + y)

    val denominator = sc.textFile(input1).map(_.split(",")).filter(_.length == 5).map(fields => {

      val nick1 = fields(0).split("\\(")(1)
      val nick2 = fields(1)
      val timeIndex = fields(2)

      val key = nick1 + "," + nick2
      //      (basenick,linknick,timeIndex,1L)
      //   val baseuserflag = 1

      (key, 1L)

    }).reduceByKey((x, y) => x + y) // calculate the denominator

    val index = denominator.join(numerator).map {

      case (key, (denominator, numerator)) => (key, denominator, numerator)

    }

    val out2 = numerator.saveAsTextFile(output2)
    val out1 = denominator.saveAsTextFile(output1)
    //      val out2 = numerator.saveAsTextFile(output2)

//    val out = index.saveAsTextFile(output1)

  }

}
