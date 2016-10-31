import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Random
import java.net.URLEncoder
import java.net.URLDecoder
import org.apache.hadoop.hdfs.server.balancer.Balancer.Matcher
import java.util.regex.Pattern
import scala.collection.immutable
import collection.immutable.HashMap

import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.sql.SQLException
import java.sql.PreparedStatement

import org.apache.spark.sql.columnar.TIMESTAMP
import java.sql.Timestamp
import java.util.Calendar

/*将tac_ua.tac.ua值结合，并计算jaccard值*/
/*
 * 输入：mapreduce(lalalalla)的输出，格式为JD_MSISDN|13876581230	windows	1
 * 输出：input_Tac和input_UA两个文件：
 * input_Tac: QQ_normal|469347685|mobile|1|15 其中15为tac_num。即出现QQ_normal|469347685的记录数
 * input_UA:net|QQ_normal|253133217|1|149，其中149为net的记录数，即出现net的记录数
 * 输出：tac + "|" + ua + "|" + tac_ua + "|" + ua_num + "|" + tac_num + "|" + jaccard
 */

object JoinTacUaNum {

  def main(args: Array[String]) = {

    if (args.length != 3) {
      sys.error("Usage:<input><output><output2>")
    }

    val Array(input, output, output2) = args

    var outputString = ""

    val conf = new SparkConf
    conf.setAppName("JoinTacUaNum") //.setMaster("local")
    val sc = new SparkContext(conf)

    //    var serialNum = 0
    val timeAndID = sc.textFile(input).map(_.split("\t")).filter(_.length == 3).map(x => {
      var tac = x(0)
      var ua = x(1)
      var tac_ua = x(2).toInt
      var key = tac
      (tac, tac_ua)
    }).reduceByKey(_ + _)
    val timeAndID1 = sc.textFile(input).map(_.split("\t")).filter(_.length == 3)
      .map(x => {
        var tac = x(0)
        var ua = x(1)
        var tac_ua = x(2).toInt
        var value = ua + "|" + tac_ua
        (tac, value)
      })
    val merge_tac = timeAndID1.join(timeAndID).map {
      case (tac, (value, tac_ua)) => (tac, value, tac_ua)
    }.map(x => s"${x._1}|${x._2}|${x._3}").saveAsTextFile(output)

    val ua = sc.textFile(input).map(_.split("\t")).filter(_.length == 3).map(x => {
      var tac = x(0)
      var ua = x(1)
      var tac_ua = x(2).toInt
      (ua, tac_ua)
    }).reduceByKey(_ + _)
    val ua1 = sc.textFile(input).map(_.split("\t")).filter(_.length == 3)
      .map(x => {
        var tac = x(0)
        var ua = x(1)
        var tac_ua = x(2).toInt
        var value = tac + "|" + tac_ua
        (ua, value)
      })
    val merge_ua = ua1.join(ua).map {
      case (ua, (value, tac_ua)) => (ua, value, tac_ua)
    }.map(x => s"${x._1}|${x._2}|${x._3}").saveAsTextFile(output2)

  }
}