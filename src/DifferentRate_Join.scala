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

object DifferentRate_Join {

  def main(args: Array[String]) {

    if (args.length != 3) {
      sys.error("Usage:<input1><input2><output>")
    }

    val Array(input1, input2, output) = args

    val sparkConf = new SparkConf().setAppName("Join") // .setMaster("local")

    sparkConf.set("spark.akka.frameSize", "100")
    val sc = new SparkContext(sparkConf)

    var userid = ""
    var itemid = ""

    val numerator = sc.textFile(input1).map(_.split(",")).filter(_.length == 3).map(fields => {

      val nick1 = fields(0).trim()
      val nick2 = fields(1).trim()
      val info = fields(2).trim()

      val key = nick1 + "," + nick2
      //      (basenick,linknick,timeIndex,1L)
      //   val baseuserflag = 1

      (key, info)

    })

    val denominator = sc.textFile(input2).map(_.split(",")).filter(_.length == 3).map(fields => {

      val nick1 = fields(0).trim()
      val nick2 = fields(1).trim()
      val count = fields(2).trim()

      var key = nick1 + "," + nick2
      //      val value =
      //      (basenick,linknick,timeIndex,1L)
      //   val baseuserflag = 1

      (key, count)

    })

    val joinOnlineUser = denominator.join(numerator).map {

      case (key, (count, info)) => (key, count,info)

    }


    val out1 = joinOnlineUser.saveAsTextFile(output)
    //      val out2 = numerator.saveAsTextFile(output2)

    //    val out = index.saveAsTextFile(output1)

  }

}
