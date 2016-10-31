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

object extractBrowers {

  def getWindowsBrowers(ua: String): String = {
    // println("begin extract")
    //  println(ua)
    var version = ""
    var result = ""
    if (ua.contains("msie")) {

      //   println("enter")
      var regx = "msie " + "(\\d+\\.\\d+).*"
      var temp = Pattern.compile(regx).matcher(ua)
      while (temp.find()) {
        version = temp.group(1)
      }
      if (version != "") {
        result = "Window_IE_" + version
      } else {
        result = "Window_IE"
      }

    } else if ((ua.contains("rv:")) && (ua.contains("windows nt")) && (ua.contains("gecko"))) { //"windows nt"、"gecko"和“rv:”
      var regx = "rv:" + "(\\d+\\.\\d+).*"
      var temp = Pattern.compile(regx).matcher(ua)
      while (temp.find()) {
        version = temp.group(1)
      }
      if (version != "") {
        result = "Window_IE_" + version
      } else {
        result = "Window_IE"
      }
    } else if ((ua.contains("edge/")) && (ua.contains("windows nt"))) { //"windows nt"和"edge/"
      var regx = "edge/" + "(\\d+\\.\\d+).*"
      var temp = Pattern.compile(regx).matcher(ua)
      while (temp.find()) {
        version = temp.group(1)
      }
      if (version != "") {
        result = "Window_IE_" + version
      } else {
        result = "Window_IE"
      }
    } else if ((ua.contains("firefox/")) && (ua.contains("windows nt"))) {
      //  println("enter")
      var regx = "firefox/" + "(\\d+\\.\\d+).*"
      var temp = Pattern.compile(regx).matcher(ua)
      while (temp.find()) {
        version = temp.group(1)
      }
      if (version != "") {
        result = "Window_fx_" + version
      }
    } else if ((ua.contains("chrome/")) && (ua.contains("windows nt"))) { //"windows nt"和"chrome/
      var regx = "chrome/" + "(\\d+\\.\\d+).*"
      var temp = Pattern.compile(regx).matcher(ua)
      while (temp.find()) {
        version = temp.group(1)
      }
      if (version != "") {
        result = "Window_chrome_" + version
      }
    } //"windows nt"和"opera/
    else if ((ua.contains("opera/")) && (ua.contains("windows nt"))) { //"windows nt"和"chrome/
      var regx = "^opera/" + "(\\d+\\.\\d+).*"
      var temp = Pattern.compile(regx).matcher(ua)
      while (temp.find()) {
        version = temp.group(1)

      }
      if (version != "") {
        result = "Window_opera_" + version
      }
    } // "windows nt","safari/" 同时不包含 "chrome/"
    else if ((ua.contains("safari/")) && (ua.contains("windows nt")) && (!(ua.contains("chrome")))) { //"windows nt"和"chrome/
      var regx = "safari/" + "(\\d+\\.\\d+).*"
      var temp = Pattern.compile(regx).matcher(ua)
      while (temp.find()) {
        version = temp.group(1)
      }
      if (version != "") {
        result = "Window_safari_" + version
      }
    }
    //print(result)

    result
    //Mozilla/5.0 (Windows NT 6.3; WOW64; rv:45.0) Gecko/20100101 Firefox/45.0

  }

  def main(args: Array[String]) = {

    if (args.length != 2) {
      sys.error("Usage:<input><output>")
    }

    val Array(input, output) = args

    var outputString = ""

    val conf = new SparkConf
    conf.setAppName("lvqiujian-taobao-extraction") //.setMaster("local")
    conf.set("spark.akka.frameSize", "500")
    val sc = new SparkContext(conf)

    //    var serialNum = 0
    val timeAndID = sc.textFile(input)
      .map(_.split("\\|")).filter(_.length > 6)
      .map(x => {
        var ua = x(6).toString().toLowerCase()
          
          //"Mozilla/5.0 (Windows NT 6.3; Trident/7.0; rv:11.0) like Gecko"
          
          //"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36 Edge/12.0"
          
          
          //"Mozilla/5.0 (Windows NT 6.3; Trident/7.0; rv:11.0) like Gecko"

        //x(6).toString().toLowerCase()
        //                        println(ua)

        //ua ="Mozilla/5.0 (Windows; U; Windows NT 5.2) AppleWebKit/525.13 (KHTML, like Gecko) Version/3.1 Safari/525.13"

        //"Opera/8.0 (Windows NT 5.1; U; en)"

        //"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.99 Safari/537.36 2345Explorer/6.3.0.9753"
        //"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)"

        //"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:45.0) Gecko/20100101 Firefox/45.0"
        var result = ""
        result = getWindowsBrowers(ua.toLowerCase())
        if (result == "") {
          // result = ua
        } else {
          //result = result +"***"+ua
        }
        if(x(3).contains("QQ")){
          if(!(x(5).contains("qzone"))){
            result = ""
          }
        }

        (x(3), x(4), result)

        //        println("result" + result)
        //  x(0) + "|" + x(1) + "|" + x(2) + "|" + x(3) + "|" + x(4) + "|" + x(5) + "|" + result
      }).filter(_._3 != "").distinct().saveAsTextFile(output)
  }

}