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

object ExtractUa {

  val LOGTIMESTAMP_INDEX_65 = 8
  val LOGTIMESTAMP_INDEX_67 = 10 //11
  val USERIP_INDEX_65 = 15
  val USERIPv4_INDEX_67 = 16 //17
  val USERIPv6_INDEX_67 = 17 //18
  val HOST_65 = 47
  val HOST_67 = 49 //50
  val URI_INDEX_65 = 48
  val URI_INDEX_67 = 50 //51
  val COOKIE_INDEX_65 = 53
  val COOKIE_INDEX_67 = 55 //56
  val USERAGENT_67 = 52

  var OSmap = new Array[String](10)
  OSmap(0) = "iphone os"
  OSmap(1) = "cfnetwork"
  OSmap(2) = "; ios"
  OSmap(3) = "iphoneos"
  OSmap(4) = "ios-iphone"
  OSmap(5) = "cpu iphone"
  //  OSmap(6) = " OS"
  val IPHONECNT = 6

  var androimap = new Array[String](10)
  androimap(0) = "android os ; " //(m1 note; android; android os ; 4.4.4; zh
  androimap(1) = "android os" //android; android os 4.4.4; zh_cn
  androimap(2) = "android"
  val ANDROIDCNT = 3

  var WTmap = new Array[String](10)
  WTmap(0) = "windows nt" //(m1 note; android; android os ; 4.4.4; zh
  //  WTmap(1) = "android os" //android; android os 4.4.4; zh_cn
  //  WTmap(2) = "android"
  val WTCNT = 1
  
//  var macosxmap = new Array[String](10)
//  macosxmap(0) = "macosx"
//  macosxmap(1) = "mac os x"
//  val MACOSXCNT = 2
//  
//  var chromeosmap = new Array[String](10)
//  chromeosmap(0) = "chromeos"
//  val CHROMEOSCNT = 1
//  
//  var WPmap = new Array[String](10)
//  WPmap(0) = "wp"
//  WPmap(1) = "Windows Phone"
//  WPmap(2) = "windows phone os"
//  val WPCNT = 3
//  
//  var Wxpmap = new Array[String](10)
//  Wxpmap(0) = "windows xp"
//  val WXPCNT = 1
//  
//  var WMmap = new Array[String](10)
//  WMmap(0) = "windows mobile"
//  WMmap(1) = "windowsmobile"
//  val WMCNT = 2
//  
//  var WCmap = new Array[String](10)
//  WCmap(0) = "windowsce"
//  WCmap(1) = "windows ce"
//  val WCCNT = 2
//  
//  var WRmap = new Array[String](10)
//  WRmap(0) = "windows rt"
//  val WRCNT = 1
//  
//  var Windowsmap = new Array[String](10)
//  Windowsmap(0) = "windows"
//  val WINDOWSCNT = 1
//  
//  var Symbianmap = new Array[String](10)
//  Symbianmap(0) = "symbianos"
//  Symbianmap(1) = "symbian"
//  val SYMBIANCNT = 2
//  
//  var Blackberrymap = new Array[String](10)
//  Blackberrymap(0) = "blackberry"
//  val BLACKBERRYCNT = 1
//  
//  var Yunosmap = new Array[String](10)
//  Yunosmap(0) = "yunos"
//  val YUNOSMAPCNT = 1
//  
//  var Meegomap = new Array[String](10)
//  Meegomap(0) = "meego"
//  val MEEGOCNT = 1
//  
//  var Smartisanmap = new Array[String](10)
//  Smartisanmap(0) = "smartisan"
//  val SMARTISANCNT = 1
//  
//  var Mauimap = new Array[String](10)
//  Mauimap(0) = "maui_wap"
//  val MAUICNT = 1
//  
//  var Nucleusmap = new Array[String](10)
//  Nucleusmap(0) = "nucleus"
//  val NUCLEUSCNT = 1
//  
//  var Mtkmap = new Array[String](10)
//  Mtkmap(0) = "mtk"
//  val MTKCNT = 1
//  
//  var Badamap = new Array[String](10)
//  Badamap(0) = "bada"
//  val BADACNT = 1
//  
//  var Webosmap = new Array[String](10)
//  Webosmap(0) = "webos"
//  val WEBOSCNT = 1
//  
//  var Palmosmap = new Array[String](10)
//  Palmosmap(0) = "palmos"
//  Palmosmap(1) = "palm os"
//  val PALMOSCNT = 2
//  
//  var Qualcommmap = new Array[String](10)
//  Qualcommmap(0) = "qualcomm"
//  val QUALCOMMMAP = 1
//  
//  var Rimmap = new Array[String](10)
//  Rimmap(0) = "rim"
//  val RIMCNT = 1
//  
//  var Hiptopmap = new Array[String](10)
//  Hiptopmap(0) = "hiptop"
//  val HIPTOPCNT = 1
//  
//  var Linuxosmap = new Array[String](10)
//  Linuxosmap(0) = "linuxos"
//  val LINUXOSCNT = 1
  def getiphoneversion(ua: String): String = {
    var version = ""
    var result = ""

    for (i <- 0 until IPHONECNT) {
      if (ua.contains(OSmap(i).toString())) {
        var regx = OSmap(i) + "[\\s*|,|;|/](\\d+[\\.|_]\\d+[\\.|_]\\d+)[,|;|\\s*|)|[a-z]]"
        var temp = Pattern.compile(regx).matcher(ua)
        while (temp.find()) {
          result = temp.group(1)
        }
        if (result == "") {
          var regx = OSmap(i) + "[\\s*|,|;|/](\\d+[\\.|_]\\d)[,|;|\\s*|)|[a-z]]"
          temp = Pattern.compile(regx).matcher(ua)
          while (temp.find()) {
            result = temp.group(1)
          }
        }
        if (result == "") {
          var regx = OSmap(i) + "[\\s*|,|;|/](\\d+)[,|;|\\s*|)|[a-z]]"
          temp = Pattern.compile(regx).matcher(ua)
          while (temp.find()) {
            result = temp.group(1)
          }
        }

      }
    }
    if (result == "") {

      for (i <- 0 until IPHONECNT) {
        if (ua.contains(OSmap(i))) {
          var regx = OSmap(i) + "(\\d+[\\.|_]\\d+[\\.|_]\\d+)[,|;|\\s*|)|[a-z]]"
          var temp = Pattern.compile(regx).matcher(ua)
          while (temp.find()) {
            result = temp.group(1)
          }
          if (result == "") {
            var regx = OSmap(i) + "(\\d+[\\.|_]\\d)[,|;|\\s*|)|[a-z]]"
            temp = Pattern.compile(regx).matcher(ua)
            while (temp.find()) {
              result = temp.group(1)
            }
          }
          if (result == "") {
            var regx = OSmap(i) + "(\\d+)[,|;|\\s*|)|[a-z]]"
            temp = Pattern.compile(regx).matcher(ua)
            while (temp.find()) {
              result = temp.group(1)
            }
          }
          if (result == "") {
            result = ua
          }

        }
      }
    }

    if ((result.matches("^(\\d+[\\.|_]\\d+[\\.|_]\\d+)$")) || (result.matches("^(\\d+[\\.|_]\\d+)$")) || (result.matches("^(\\d+)$"))) {
      if (ua.contains("cfnetwork")) {
        version = "IPHONE" + "|CFNetwork/" + result + "|" + ua
      } else {
        version = "IPHONE" + "|" + result + "|" + ua
      }

    } else {
      //      println("applebresult=" + result)
      //      println("uaapple=" + ua)
      version = "IPHONE" + "|" + "0._0._0._0" + "|" + ua
      //          result = "IPHONE" + "|" + result + "|" + ua
    }
    version

  }

  def getAndrioversion(ua: String): String = {
    var version = ""
    var result = ""
    for (i <- 0 until ANDROIDCNT) {
      if (ua.contains(androimap(i).toString())) {
        println(androimap(i))
        var regx = androimap(i) + "[\\s*|,|;|/](\\d+[\\.|_]\\d+[\\.|_]\\d+)[,|;|\\s*|)|[a-z]|\\-|_|/]"
        var temp = Pattern.compile(regx).matcher(ua)
        while (temp.find()) {
          result = temp.group(1)
        }
        if (result == "") {
          //          println("enter")
          var regx = androimap(i) + "[\\s*|,|;|/](\\d+[\\.|_]\\d+)[,|;|\\s*|)|[a-z]]"
          //          println("regx=" + regx)
          temp = Pattern.compile(regx).matcher(ua)
          while (temp.find()) {
            result = temp.group(1)
          }
        }

      }
    }
    if (result == "") {

      for (i <- 0 until ANDROIDCNT) {
        if (ua.contains(androimap(i))) {
          var regx = androimap(i) + "(\\d+[\\.|_]\\d+[\\.|_]\\d+)[,|;|\\s*|)|[a-z]|-|_|/]"
          var temp = Pattern.compile(regx).matcher(ua)
          while (temp.find()) {
            result = temp.group(1)
          }
          if (result == "") {
            var regx = androimap(i) + "(\\d+[\\.|_]\\d)[,|;|\\s*|)|[a-z]]"
            temp = Pattern.compile(regx).matcher(ua)
            while (temp.find()) {
              result = temp.group(1)
            }
          }
          if (result == "") {
            result = ua
          }

        }
      }
    }

    if ((result.matches("^(\\d+[\\.|_]\\d+[\\.|_]\\d+)$")) || (result.matches("^(\\d+[\\.|_]\\d+)$")) || (result.matches("^(\\d+)$"))) {

      version = "Android" + "|" + result + "|" + ua

    } else {
      //      println("abresult=" + result)
      //      println("ua=" + ua)
      version = "Android" + "|" + "0._0._0._0" + "|" + ua
      //          result = "IPHONE" + "|" + result + "|" + ua
    }

    version

  }
  def getWTversion(ua: String): String = {
    var version = ""
    var result = ""
    for (i <- 0 until WTCNT) {
      if (ua.contains(WTmap(i).toString())) {
        println(WTmap(i))
        var regx = WTmap(i) + "[\\s*|,|;|/](\\d+[\\.|_]\\d+[\\.|_]\\d+)[,|;|\\s*|)|[a-z]|\\-|_|/]"
        var temp = Pattern.compile(regx).matcher(ua)
        while (temp.find()) {
          result = temp.group(1)
        }
        if (result == "") {
          //          println("enter")
          var regx = WTmap(i) + "[\\s*|,|;|/](\\d+[\\.|_]\\d+)[,|;|\\s*|)|[a-z]]"
          //          println("regx=" + regx)
          temp = Pattern.compile(regx).matcher(ua)
          while (temp.find()) {
            result = temp.group(1)
          }
        }

      }
    }
    if (result == "") {

      for (i <- 0 until WTCNT) {
        if (ua.contains(WTmap(i))) {
          var regx = WTmap(i) + "(\\d+[\\.|_]\\d+[\\.|_]\\d+)[,|;|\\s*|)|[a-z]|-|_|/]"
          var temp = Pattern.compile(regx).matcher(ua)
          while (temp.find()) {
            result = temp.group(1)
          }
          if (result == "") {
            var regx = WTmap(i) + "(\\d+[\\.|_]\\d)[,|;|\\s*|)|[a-z]]"
            temp = Pattern.compile(regx).matcher(ua)
            while (temp.find()) {
              result = temp.group(1)
            }
          }
          if (result == "") {
            result = ua
          }

        }
      }
    }

    if ((result.matches("^(\\d+[\\.|_]\\d+[\\.|_]\\d+)$")) || (result.matches("^(\\d+[\\.|_]\\d+)$")) || (result.matches("^(\\d+)$"))) {

      version = "WT" + "|" + result + "|" + ua

    } else {
      println("abresult=" + result)
      println("ua=" + ua)
      version = "WT" + "|" + "0._0._0._0" + "|" + ua
      //          result = "IPHONE" + "|" + result + "|" + ua
    }

    version

  }
  
  def getMacosxversion(ua: String): String = {
    var version = ""
    var result = ""
    if(ua.contains("macosx")||ua.contains("mac os x")){
      version = "Macosx" + "|" + "0.0.0.0" + "|" + ua
    }else{
      version = "Macosx" + "|" + "0._0._0._0" + "|" + ua
    }
      version
  }
  
   def getChromeosversion(ua: String): String = {
    var version = ""
    var result = ""
    if(ua.contains("chromeos")){
      version = "Chromeos" + "|" + "0.0.0.0" + "|" + ua
    }else{
      version = "Chromeos" + "|" + "0._0._0._0" + "|" + ua
    }
      version
  }
   
   def getWPversion(ua: String): String = {
    var version = ""
    var result = ""
    if(ua.contains("Windows Phone")||ua.contains("windows phone os")){
      version = "WP" + "|" + "0.0.0.0" + "|" + ua
    }else{
      version = "WP" + "|" + "0._0._0._0" + "|" + ua
    }
      version
  }
   
    def getWxpversion(ua: String): String = {
    var version = ""
    var result = ""
    if(ua.contains("windows xp")){
      version = "Wxp" + "|" + "0.0.0.0" + "|" + ua
    }else{
      version = "Wxp" + "|" + "0._0._0._0" + "|" + ua
    }
      version
  }
   
    def getWMversion(ua: String): String = {
    var version = ""
    var result = ""
    if(ua.contains("windows mobile")||ua.contains("windowsmobile")){
      version = "WM" + "|" + "0.0.0.0" + "|" + ua
    }else{
      version = "WM" + "|" + "0._0._0._0" + "|" + ua
    }
      version
  }
   
    def getWCversion(ua: String): String = {
    var version = ""
    var result = ""
    if(ua.contains("windowsce")||ua.contains("windows ce")){
      version = "WC" + "|" + "0.0.0.0" + "|" + ua
    }else{
      version = "WC" + "|" + "0._0._0._0" + "|" + ua
    }
      version
  }
    
    def getWRversion(ua: String): String = {
    var version = ""
    var result = ""
    if(ua.contains("windows rt")){
      version = "WR" + "|" + "0.0.0.0" + "|" + ua
    }else{
      version = "WR" + "|" + "0._0._0._0" + "|" + ua
    }
      version
  }
    
    def getWindowsversion(ua: String): String = {
    var version = ""
    var result = ""
    if(ua.contains("windows")){
      version = "Windows" + "|" + "0.0.0.0" + "|" + ua
    }else{
      version = "Windows" + "|" + "0._0._0._0" + "|" + ua
    }
      version
  }
    
    def getSymbianversion(ua: String): String = {
    var version = ""
    var result = ""
    if(ua.contains("symbian")||ua.contains("symbianos")){
      version = "Symbian" + "|" + "0.0.0.0" + "|" + ua
    }else{
      version = "Symbian" + "|" + "0._0._0._0" + "|" + ua
    }
      version
  }
    
    def getBlackberryversion(ua: String): String = {
    var version = ""
    var result = ""
    if(ua.contains("blackberry")){
      version = "Blackberry" + "|" + "0.0.0.0" + "|" + ua
    }else{
      version = "Blackberry" + "|" + "0._0._0._0" + "|" + ua
    }
      version
  }
   
    
    def getYunosversion(ua: String): String = {
    var version = ""
    var result = ""
    if(ua.contains("yunos")){
      version = "Yunos" + "|" + "0.0.0.0" + "|" + ua
    }else{
      version = "Yunos" + "|" + "0._0._0._0" + "|" + ua
    }
      version
  }
    
    def getMeegoversion(ua: String): String = {
    var version = ""
    var result = ""
    if(ua.contains("meego")){
      version = "MeeGo" + "|" + "0.0.0.0" + "|" + ua
    }else{
      version = "MeeGo" + "|" + "0._0._0._0" + "|" + ua
    }
      version
  }
    
    def getSmartisanversion(ua: String): String = {
    var version = ""
    var result = ""
    if(ua.contains("smartisan")){
      version = "Smartisan" + "|" + "0.0.0.0" + "|" + ua
    }else{
      version = "Smartisan" + "|" + "0._0._0._0" + "|" + ua
    }
      version
  }    
    
    def getMauiversion(ua: String): String = {
    var version = ""
    var result = ""
    if(ua.contains("maui_wap")){
      version = "Maui" + "|" + "0.0.0.0" + "|" + ua
    }else{
      version = "Maui" + "|" + "0._0._0._0" + "|" + ua
    }
      version
  }  
    
    def getNucleusversion(ua: String): String = {
    var version = ""
    var result = ""
    if(ua.contains("nucleus")){
      version = "Nucleus" + "|" + "0.0.0.0" + "|" + ua
    }else{
      version = "Nucleus" + "|" + "0._0._0._0" + "|" + ua
    }
      version
  }  
    
    def getMtkversion(ua: String): String = {
    var version = ""
    var result = ""
    if(ua.contains("mtk")){
      version = "MTK" + "|" + "0.0.0.0" + "|" + ua
    }else{
      version = "MTK" + "|" + "0._0._0._0" + "|" + ua
    }
      version
  } 
    
    def getBadaversion(ua: String): String = {
    var version = ""
    var result = ""
    if(ua.contains("bada")){
      version = "BadaOS" + "|" + "0.0.0.0" + "|" + ua
    }else{
      version = "BadaOS" + "|" + "0._0._0._0" + "|" + ua
    }
      version
  } 
    
    def getWebosversion(ua: String): String = {
    var version = ""
    var result = ""
    if(ua.contains("webos")){
      version = "WebOS" + "|" + "0.0.0.0" + "|" + ua
    }else{
      version = "WebOS" + "|" + "0._0._0._0" + "|" + ua
    }
      version
  } 
    
    def getPalmosversion(ua: String): String = {
    var version = ""
    var result = ""
    if(ua.contains("palmos")||ua.contains("palm os")){
      version = "PalmOS" + "|" + "0.0.0.0" + "|" + ua
    }else{
      version = "PalmOS" + "|" + "0._0._0._0" + "|" + ua
    }
      version
  } 
    
    def getQualcommversion(ua: String): String = {
    var version = ""
    var result = ""
    if(ua.contains("qualcomm")){
      version = "QualcommOS" + "|" + "0.0.0.0" + "|" + ua
    }else{
      version = "QualcommOS" + "|" + "0._0._0._0" + "|" + ua
    }
      version
  } 
    
    def getRimversion(ua: String): String = {
    var version = ""
    var result = ""
    if(ua.contains("rim")){
      version = "Rim" + "|" + "0.0.0.0" + "|" + ua
    }else{
      version = "Rim" + "|" + "0._0._0._0" + "|" + ua
    }
      version
  } 
    
    def getHiptopversion(ua: String): String = {
    var version = ""
    var result = ""
    if(ua.contains("hiptop")){
      version = "HiptopOS" + "|" + "0.0.0.0" + "|" + ua
    }else{
      version = "HiptopOS" + "|" + "0._0._0._0" + "|" + ua
    }
      version
  } 
    
    def getLinuxosversion(ua: String): String = {
    var version = ""
    var result = ""
    if(ua.contains("linuxos")){
      version = "LinuxOS" + "|" + "0.0.0.0" + "|" + ua
    }else{
      version = "LinuxOS" + "|" + "0._0._0._0" + "|" + ua
    }
      version
  } 
    
  def main(args: Array[String]) = {

    if (args.length != 2) {
      sys.error("Usage:<input><output>")
    }

    val Array(input, output) = args

    var outputString = ""

    val conf = new SparkConf
    conf.setAppName("os-extraction")//.setMaster("local")
    val sc = new SparkContext(conf)

    //    var serialNum = 0
    val timeAndID = sc.textFile(input)
      .map(_.split("\\|")).filter(_.length > 6)
      .map(x => {
        //        var ua = x.toString().toLowerCase()
        var ua = x(6).toString().toLowerCase()
        //                        println(ua)

        //        ua = "(mozilla/4.0 (compatible;android;320x480),117.136.240.193"
        var result = ""
        result = getiphoneversion(ua)
        if (result.contains("0._0._0._0")) {
          result = getAndrioversion(ua)
        }
        if (result.contains("0._0._0._0")) {
          result = getWTversion(ua)
        }
        if (result.contains("0._0._0._0")) {
          result = getMacosxversion(ua)
        }
        if (result.contains("0._0._0._0")) {
          result = getChromeosversion(ua)
        }
        if (result.contains("0._0._0._0")) {
          result = getWPversion(ua)
        }
        if (result.contains("0._0._0._0")) {
          result = getWxpversion(ua)
        }
        if (result.contains("0._0._0._0")) {
          result = getWMversion(ua)
        }
        if (result.contains("0._0._0._0")) {
          result = getWCversion(ua)
        }
        if (result.contains("0._0._0._0")) {
          result = getWRversion(ua)
        }
        if (result.contains("0._0._0._0")) {
          result = getWindowsversion(ua)
        }
        if (result.contains("0._0._0._0")) {
          result = getSymbianversion(ua)
        }
        if (result.contains("0._0._0._0")) {
          result = getBlackberryversion(ua)
        }
        if (result.contains("0._0._0._0")) {
          result = getYunosversion(ua)
        }
        if (result.contains("0._0._0._0")) {
          result = getMeegoversion(ua)
        }
        if (result.contains("0._0._0._0")) {
          result = getSmartisanversion(ua)
        }
        if (result.contains("0._0._0._0")) {
          result = getMauiversion(ua)
        }
        if (result.contains("0._0._0._0")) {
          result = getNucleusversion(ua)
        }
        if (result.contains("0._0._0._0")) {
          result = getMtkversion(ua)
        }
        if (result.contains("0._0._0._0")) {
          result = getBadaversion(ua)
        }
        if (result.contains("0._0._0._0")) {
          result = getWebosversion(ua)
        }
        if (result.contains("0._0._0._0")) {
          result = getPalmosversion(ua)
        }
        if (result.contains("0._0._0._0")) {
          result = getQualcommversion(ua)
        }
        if (result.contains("0._0._0._0")) {
          result = getRimversion(ua)
        }
        if (result.contains("0._0._0._0")) {
          result = getHiptopversion(ua)
        }
        if (result.contains("0._0._0._0")) {
          result = getLinuxosversion(ua)
        }

        //        println("result" + result)
//        x(0) + "|" + x(1) + "|" + x(2) + "|" + x(3) + "|" + x(4) + "|" + x(5) + "|" + result
//         result
         x(0) + "|" + x(1) + "|" + x(2) + "|" + x(3) + "|" + x(4) + "|" + x(5) + "|" + result
        
      }).saveAsTextFile(output)

  }
}