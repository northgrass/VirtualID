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

object JoinWeekRemain {

  def main(args: Array[String]) {

    if (args.length != 3) {
      sys.error("Usage:<input1><input2><output>")
    }

    val Array(input1, input2, output) = args

    val sparkConf = new SparkConf().setAppName("Join") //.setMaster("local")

    sparkConf.set("spark.akka.frameSize", "100")
    val sc = new SparkContext(sparkConf)


    val remain_id = sc.textFile(input1).filter(!_.contains("None")).map(_.split(",")).filter(_.length == 5).map(fields => {

      val nick1 = fields(0).replace("(", "").trim()
      val nick2 = fields(1).trim()
      val key = nick1 + "," + nick2
      val info = fields(2).replace("Some(", "").trim() + "," + fields(3).trim() + "," + fields(4).replace(")", "").trim()
      //      (basenick,linknick,timeIndex,1L)
      //   val baseuserflag = 1

      (key, info)

    })

    val weekday = sc.textFile(input2).map(_.split(",")).filter(_.length == 3).map(fields => {

      val nick = fields(0).trim() + "," +fields(1).trim()
      val count = ""

      var key = nick
      //      val value =
      //      (basenick,linknick,timeIndex,1L)
      //   val baseuserflag = 1

      (key, count)

    })
    
    val weekdayOppo = sc.textFile(input2).map(_.split(",")).filter(_.length == 3).map(fields => {

      val nick = fields(1).trim() + "," +fields(0).trim()
      val count = ""

      var key = nick
      //      val value =
      //      (basenick,linknick,timeIndex,1L)
      //   val baseuserflag = 1

      (key, count)

    })
    
    val weekdayAll = weekday.union(weekdayOppo).distinct
    

    val joinUser = remain_id.join(weekdayAll).distinct.map {

      case (key, (info, count)) => (key,info)

    }


    val out1 = joinUser.saveAsTextFile(output)
    //      val out2 = numerator.saveAsTextFile(output2)

    //    val out = index.saveAsTextFile(output1)

  }

}
