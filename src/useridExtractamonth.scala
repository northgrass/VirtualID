import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Random
import java.net.URLEncoder
import java.net.URLDecoder
import org.apache.hadoop.hdfs.server.balancer.Balancer.Matcher
import java.util.regex.Pattern
import scala.collection.immutable
import collection.immutable.HashMap
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.sql.SQLException
import java.sql.PreparedStatement

import org.apache.spark.sql.columnar.TIMESTAMP
import java.sql.Timestamp
import java.util.Calendar

object useridExtractamonth {

  //  /*procedure endtime*/
  /*for lablog*/
  //  val LOGTIMESTAMP_INDEX_65 = 8
  //  val LOGTIMESTAMP_INDEX_67 = 10
  //  val USERIP_INDEX_67 = 16
  //  val USERIP_INDEX_65 = 14
  //  val HOST_65 = 47
  //  val HOST_67 = 49
  //  val URI_INDEX_65 = 48
  //  val URI_INDEX_67 = 50
  //  val COOKIE_INDEX_65 = 53
  //  val COOKIE_INDEX_67 = 55

  /*for xdr*/

  val LOGTIMESTAMP_INDEX_14 = 1 //2
  val USERIPv4_INDEX_14 = 0 //1
  val HOST_14 = 3 //4
  val URI_INDEX_14 = 4 //5
  val COOKIE_INDEX_14 = 7 //8
  val USERAGENT_14 = 6 //7

  //18
  def taobao(uri: String, cookie: String): String = {
    //    println("deal with taobao")

    var nick = ""

    var nicktype = ""

    /*phone*/
    val taobao_phone1 = "&[uU][Ss][Ee][Rr][iI][dD]=cntaobao" //[uU][sS][eE][rR][iI][Dd]  
    //    val taobao_phone2 = "&user_id=cntaobao"
    //    val taobao_phone3 = "data=%7B%22*nick%22%3A%22" //match  data={"nick":"qiuchengkai4","use  +  data={"usernick":"qiuchengkai4","use
    //    val taobao_phone4 = "%22%2C%22nick%22%3A%22" //match  ","nick":"xxxxx","
    val taobao_phone5 = "&nick=" //match &nick=\u8349\u672C110& limited by uri
    //  val taobao_phone6 = "%22%2C%5C%22usernick%5C%22%3A%5C%22" //match \"usernick\":\"鐏忓话鐎规紭2008\"}"
    /*web*/
    val taobao_web1 = "&loginid=cntaobao" //   &loginid=cntaobao

    val taobao_web2 = "&uid=cntaobao" // limited by uri &uid=cntaobao, ignore uid.and care about loginId
    /*deal with uri*/
    if (uri.split(taobao_phone1).length > 1) { ///ww.1.23.1.1.28,431  and  /api/v2/account/device_token.json,20
      if (uri.contains("&wwnick=cntaobao") && (uri.contains("&userid="))) {
        val nickString = uri.split("&wwnick=cntaobao")(1)
        nick = (nickString.split("&"))(0) + ","
        nicktype = "1"
      }

      val nickString = uri.split(taobao_phone1)(1)
      nick = nick + (nickString.split("&"))(0)
      nicktype = "1"
    } //    else if (uri.split(taobao_phone2).length > 1) {
    //      val nickString = uri.split(taobao_phone2)(1)
    //      nick = (nickString.split("&"))(0)
    //      nicktype = "2"
    //    } 
    //    else if (uri.split(taobao_phone3).length > 1) {
    //      val nickString = uri.split(taobao_phone3)(1)
    //      nick = (nickString.split("%22"))(0) // to avoid truncate閿涘瘈se %22 to represent %22%2C%22, which means (:)
    //      nicktype = "3"
    //    } 
    //    else if (uri.split(taobao_phone4).length > 1) {
    //      val nickString = uri.split(taobao_phone4)(1)
    //      nick = nickString.split("%22")(0)
    //      nicktype = "4"
    //    }
    else if (uri.split(taobao_phone5).length > 1) { // need to be limited by uri
      if ((uri.contains("/rpc/history")) || (uri.contains("/version/check")) || (uri.contains("/approuter"))) {
        val nickString = uri.split(taobao_phone5)(1)
        nick = nickString.split("&")(0)
        nicktype = "5"
      }
    } //    else if (uri.split(taobao_phone6).length > 1) {
    //      val nickString = uri.split(taobao_phone6)(1)
    //      nick = nickString.split("%5C")(0)
    //      nicktype = "6"
    //    }
    else if (uri.split(taobao_web1).length > 1) {
      val nickString = uri.split(taobao_web1)(1) //"&loginid=cntaobao"
      nick = (nickString.split("&")(0))
      nicktype = "7"
    } else if (uri.split(taobao_web2).length > 1) {
      if ((uri.contains("/redirect")) || (uri.contains("/notify_ok_v2"))) {
        val nickString = uri.split(taobao_web2)(1) //"&loginid=cntaobao"
        nick = (nickString.split("&")(0))
        nicktype = "12"
      }
    } else {
      nick = ""
    }
    /*deal with cookie*/
    if ((nick == "") && (cookie != "")) {
      if (cookie.split("lgc=").length > 1) {
        val nickString = cookie.split("lgc=")(1)
        if ((nickString.split(";").length > 0) && (nickString.split(";")(0) != "")) {
          nick = (nickString.split(";"))(0)
          nicktype = "8"
        }
      } else if (cookie.split("tracknick=").length > 1) {
        val nickString = cookie.split("tracknick=")(1)
        if ((nickString.split(";").length > 0) && (nickString.split(";")(0) != "")) {
          nick = (nickString.split(";"))(0)
          nicktype = "9"
        }
      } else if (cookie.split("_w_tb_nick=").length > 1) {
        val nickString = cookie.split("_w_tb_nick=")(1)
        if ((nickString.split(";").length > 0) && (nickString.split(";")(0) != "")) {
          nick = (nickString.split(";"))(0)
          nicktype = "10"
        }
      } else if (cookie.split("_nk=").length > 1) {
        val nickString = cookie.split("_nk=")(1)
        if ((nickString.split(";").length > 0) && (nickString.split(";")(0) != "")) {
          nick = (nickString.split(";"))(0)
          nicktype = "11"
        }
      } else {
        nick = ""
      }
    }

    if ((nick != "") && (nick != "0")) {
      if ((nick.length() > 250) || (nick.contains("\\/"))) {
        nick = "abnormalNick"
      }

      nick = nick
    } else {
      nick = ""
      nicktype = ""
    }

    //    println(nick)

    nick
  }

  def JD(uri: String, cookie: String): String = {
    var nick = ""
    var linknick = ""
    var nickSource = ""
    /*Uri*/
    /*cookie*/
    val jdweb_ck2 = "^(\\s*)_pst=.*"
    val jdphone_web__ck1 = "^(\\s*)pin=.*"
    val jdphone_web__uri = "pin=.*"

    val linkedphone_mail_ck1 = "(\\s*)mp=.*"
    val linkedphone_mail_ck2 = "(\\s*)alpin=.*"

    val uriElement = cookie.split(";")
    var k = 0
    while (k < uriElement.length) {

      if (uriElement(k).matches(jdweb_ck2)) {
        if (uriElement(k).split("=").length > 1) {
          nick = uriElement(k).split("=")(1)
          nickSource = cookie
          if ((nick == "") || (nick == "-") || (nick == "0")) {
            nick = ""
          }
        }
      } else if (uriElement(k).matches(jdphone_web__ck1)) {
        if (uriElement(k).split("=").length > 1) {
          nick = uriElement(k).split("=")(1)
          nickSource = cookie
          if ((nick == "") || (nick == "-") || (nick == "0")) {
            nick = ""
          }
        }

      }

      /*linked phone or mail*/

      if (uriElement(k).matches(linkedphone_mail_ck1)) {
        if (uriElement(k).split("=").length > 1) {
          linknick = uriElement(k).split("=")(1)
          if ((linknick == "") || (linknick == "-") || (linknick == "0")) {
            linknick = ""
          }
        }

      } else if (uriElement(k).matches(linkedphone_mail_ck2)) {
        if (uriElement(k).split("=").length > 1) {
          linknick = uriElement(k).split("=")(1)
          if ((linknick == "") || (linknick == "-") || (linknick == "0")) {
            linknick = ""
          }
        }

      }
      k += 1

    }
    if (nick == "") {
      if (uri.split(jdphone_web__uri).length > 1) {
        val nickString = uri.split(jdphone_web__uri)(1)
        nick = nickString.split("&")(0)
        nickSource = uri
        //+ uri + "&&1"
        if ((nick == "") || (nick == "-") || (nick == "0")) {
          nick = ""
        }
      } else {
        nick = ""
      }
    }
    /*cookie*/

    if ((nick.length() > 250) || (nick.contains("\\/"))) {
      nick = "abnormalNick"
    }
    if ((linknick.length() > 250) || (linknick.contains("\\/"))) {
      linknick = "00"
    }
    //    println(nick + "," + linknick)
    nick + "," + linknick
  }

  def SianWeiboNum(uri: String, cookie: String): String = {
    //uri_phone_web1閿涳拷濞ｈ濮瀠ri閻ㄥ嫰妾洪崚锟?
    //   /adfront/deliver
    // /interface/pcright/pcright_topic.php,73
    // /interface/f/ttt/v3/pc_win.php,33
    // /interface/win/winad.php,4
    // /interface/f/ttt/v3/wbpullad.php,2

    var nick = ""
    var typenick = ""
    //    val uritest = "/wbsso/login?ssosavestate=1481193507&url=http%3A%2F%2Fweibo.com%2Fu%2F1881950540&ticket=ST-MTg4MTk1MDU0MA==-1449657507-ja-659CB26012365929244AEBDB06DF9EC3&retcode=0"
    //    val cookietest = "SINAGLOBAL=6799512030556.798.1449023963180; wvr=6; ULV=1449064118722:2:2:2:6466218349523.843.1449064118604:1449023964090; UOR=www.nenew.net,widget.weibo.com,www.ourunix.org; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WWqy0Y1HnYo3b9ARVVAJ4zc5JpX5KMt; SUHB=00x4kjguTiTfiQ; SRT=E.vAfoKDXoKO4niOAnveEdAvmBvXvCvXMdSyluvnmCBvzvvv4mV60fDXRmvvvmBnVmPc!kKAXFvRvvvXAGCXzvBvrBintBvA0BvMmCvvzRvAv7*B.vAflW-P9Rc0lR-ykADvnJqiQVbiRVPBtS!r3JZPQVqbgVdWiMZ4siOzu4DbmKPWQVQ!ndOEzUbb6J-zuAPHdP!E1iG9Zi49ndDPIOQA7;SRF=1449647033"
    val uri_phone_web1 = "[&?]uid=" //match &uid=1028511994&  
    val uri_web1 = ".*/u/(\\d+)/home.*" //match /u/2314918094/home% 
    val uri_web1_1 = ".*%2Fu%2F(\\d+)%2Fhome.*" //or %2Fu%2F1879752894%2Fhome%,,,%2Fu%2F1879752894%2F
    val cookie_web1 = "%26uid%3D" // match %26uid%3D1879752894%26
    val cookie_web2 = "SUS=SID-" //SUS=SID-1879752894-1448851233-XD-8eipa-0ec00be52669d1bc4171e710b7528571;
    val uri_web2 = "::\\d{8,}::" //match //::5389696901::,8self define??
    val uri_web3 = "uid%3A" //match :uid%3A1879752894 rs:
    val uri_web4 = "un:" //un:9:braveman2012.%2A%2A:1880890794::
    val uri_web5 = "\\d:.*" // 4:braveman2012.%2A%2A:1880890794:: 

    var urimain = ""
    if (uri.split("\\?").length > 1) {
      urimain = uri.split("\\?")(0)
      if ((urimain.matches("^(\\s*)/adfront/deliver.*")) || (urimain.matches("^(\\s*)/interface/.*"))) {
        urimain = urimain
      } else {
        urimain = ""
      }
    }
    if (urimain != "") {
      if (uri.split(uri_phone_web1).length > 1) {
        val nickString = uri.split(uri_phone_web1)(1)
        if ((nickString.contains("&")) && (nickString != "&")) { //be careful of split
          nick = nickString.split("&")(0) //nicknumber
          typenick = "1"
          if ((nick.split("[_,]").length > 1) && (nick.length() > 1)) { //be careful of &uid=xxxxx,bbbb_aaaa
            nick = nickString.split("[_,]")(0) //nicknumber
            typenick = "2"
          }
        }
        //        println(nick)
      }
    } else if (uri.matches(".*/u/(\\d+)/home.*") == true) { //contains do not need \\
      //      val nickString = uri.split(uri_web1)(1)
      nick = uri.replaceAll(".*/u/(\\d+)/home.*", "$1")
      typenick = "3"

    } else if (uri.matches(".*%2Fu%2F(\\d+)%2Fhome.*") == true) { //contains do not need \\
      //      val nickString = uri.split(uri_web1)(1)
      nick = uri.replaceAll(".*%2Fu%2F(\\d+)%2Fhome.*", "$1")
      typenick = "4"

    } else if (uri.split(uri_web3).length > 1) {
      val nickString = uri.split(uri_web3)(1)
      if ((nickString.contains(" ")) && (nickString != " ")) { //be careful of split
        nick = nickString.split(" ")(0) //nicknumber
        typenick = "5"
      }
    } else {
      val uriElement = uri.split(" ")
      var k = 0
      while (k < uriElement.length) {
        if ((uriElement(k).contains(uri_web4)) && (uriElement(k).split(":").length > 3)) {
          val nicktemp = uriElement(k).split(":")(3)
          if ((nicktemp.matches("^(\\s*)(\\d){5,}$")) && (nicktemp.length() >= nick.length())) {
            nick = nicktemp
            typenick = "6"
          }
        } else if ((uriElement(k).matches(uri_web5)) && (uriElement(k).split(":").length > 2)) {
          val nicktemp = uriElement(k).split(":")(2)
          if ((nicktemp.matches("^(\\s*)(\\d){5,}$")) && (nicktemp.length() >= nick.length())) {
            nick = nicktemp
            typenick = "7"

          }
        } else if ((uriElement(k).matches(uri_web2)) && (uriElement(k).split(":").length > 2)) {
          val nicktemp = uriElement(k).split(":")(2)
          if ((nicktemp.matches("^(\\s*)(\\d){5,}$")) && (nicktemp.length() >= nick.length())) {
            nick = nicktemp
            typenick = "8"
          }
        }
        k = k + 1
      }
    }
    if (nick == "") {
      if (cookie.split(cookie_web2).length > 1) {
        val nickString = cookie.split(cookie_web2)(1)
        if (nickString.contains("-") && (nickString != "-")) {
          nick = nickString.split("-")(0)
          typenick = "9"
        }
      } else if (cookie.split(cookie_web1).length > 1) {
        val nickString = cookie.split(cookie_web1)(1)
        if ((nickString.contains("%26")) && (nickString != "%26")) { //be careful of split
          nick = nickString.split("%26")(0)
          typenick = "10"
        }
      }
    }

    if ((nick != "") && (nick != "0")) {
      if ((nick.length() > 250) || (nick.matches("^(\\s*)(\\d){5,}$") != true)) {
        nick = "abnormalNick"
      }

      nick = "sinaWeibo_ID" + "," + nick
    } else {
      nick = ""
      typenick = ""
    }
    //        println("nick=" + nick)
    //    nick + "&&" + typenick
    nick
  }

  def SianWeiboRegisterNum(uri: String, cookie: String): String = {
    var nick = ""
    var nicktype = ""
    //    val cookieTest = ""
    //    val uritest = " lv:1448850043325:466:134:10  9:braveman2012.%2A%2A:1880890794:: uo: ae: lu: si:uid%3A1880890794 rs:0 dm:1 su:0033WrSXqPxfM725Ws9jqgMF55529P9D9W5gs0JBH5xGdZQ92UeWALRI&MT=&EX=ex1: ex2:&gUid_1448878355001"
    val web_cookie1 = "SUP=" //SUP=cv%3D1%26bt%3D1448852497%26et%3D1448938897%26d%3Dc909%26i%3D8571%26us%3D1%26vf%3D0%26vt%3D0%26ac%3D0%26st%3D0%26uid%3D1879752894%26name%3D335848046%2540qq.com%26nick%3D335848046%26fmp%3D%26lcp%3D;
    val separator = ";"
    val sec_sep = "%26"

    val web_cookie2 = " un=" // un=749840122@qq.com;(has a space in the beginning)

    val uri_web1 = "un:" //un:9:braveman2012.%2A%2A:1880890794
    val uri_web2 = "\\d:.*" // 4:braveman2012.%2A%2A:1880890794(has a space in the beginning)
    val uri_separator = " "

    val cookieElement = cookie.split(separator)
    var i = 0
    while (i < cookieElement.length) {
      var nicktemp = ""
      if (cookieElement(i).contains(web_cookie1)) { //later

        val secCookie = cookieElement(i).split(sec_sep)
        var j = 0
        while (j < secCookie.length) {

          if ((secCookie(j).contains("name%3D"))) { // name is later
            if ((secCookie(j).split("%3D").length > 1) && (secCookie(j).split("%3D")(1) != "")) {
              nicktemp = secCookie(j).split("%3D")(1)
              if (nicktemp.length() >= nick.length()) {
                nick = nicktemp //+ cookie
                nicktype = "5"
              }
            }
          }
          if ((secCookie(j).contains("user%3D"))) {
            if ((secCookie(j).split("%3D").length > 1) && (secCookie(j).split("%3D")(1) != "")) {
              nicktemp = secCookie(j).split("%3D")(1)
              if (nicktemp.length() >= nick.length()) {
                nick = nicktemp //+ cookie
                nicktype = "4"
              }
            }
          }
          j += 1
        }
      } else if (cookieElement(i).contains(web_cookie2)) {
        if ((cookieElement(i).split("=").length > 1) && (cookieElement(i).split("=")(1) != "")) {
          nicktemp = cookieElement(i).split("=")(1)
          if (nicktemp.length() >= nick.length()) {
            nick = nicktemp
            nicktype = "3"
          }
        }
      }

      i += 1
    }
    /*uri*/
    //if(nick==""){
    val uriElement = uri.split(uri_separator)
    var k = 0
    while (k < uriElement.length) {
      if ((uriElement(k).contains(uri_web1)) && (uriElement(k).split(":").length > 2)) {
        val nicktemp = uriElement(k).split(":")(2)
        if (nicktemp.length() >= nick.length()) {
          nick = nicktemp
          nicktype = "2"

        }
      } else if ((uriElement(k).matches(uri_web2)) && (uriElement(k).split(":").length > 1)) {
        val nicktemp = uriElement(k).split(":")(1)
        if (nicktemp.length() >= nick.length()) {
          nick = nicktemp
          nicktype = "1"
        }
      }
      k = k + 1
    }

    if ((nick != "") && (nick != " ")) {
      if ((nick.length() > 250) || (nick.contains("\\/"))) {
        nick = "abnormalNick"
      }
      nick = "sinaWeibo_Mail" + "," + nick

    } else {
      nick = ""
    }
    //    println("sinaMail" + nick)
    nick // + "&&" + nicktype
  }

  def SianWeibonick(uri: String, cookie: String): String = {
    //  println("nick==")
    var nick = ""
    val web_cookie1 = "SUP=" //SUP=cv%3D1%26bt%3D1448852497%26et%3D1448938897%26d%3Dc909%26i%3D8571%26us%3D1%26vf%3D0%26vt%3D0%26ac%3D0%26st%3D0%26uid%3D1879752894%26name%3D335848046%2540qq.com%26nick%3D335848046%26fmp%3D%26lcp%3D;
    val separator = ";"
    val sec_sep = "%26"
    val cookieElement = cookie.split(separator)
    var i = 0
    while (i < cookieElement.length) {
      var nicktemp = ""
      if (cookieElement(i).contains(web_cookie1)) { //later

        val secCookie = cookieElement(i).split(sec_sep)
        var j = 0
        while (j < secCookie.length) {

          if ((secCookie(j).contains("nick%3D"))) { // name is later
            if ((secCookie(j).split("%3D").length > 1) && (secCookie(j).split("%3D")(1) != "")) {
              nicktemp = secCookie(j).split("%3D")(1)
              if (nicktemp.length() >= nick.length()) {
                nick = nicktemp
              }
            }
          }
          j += 1
        }
      }
      i += 1
    }
    if ((nick != "") && (nick != " ")) {
      if ((nick.length() > 250) || (nick.contains("\\/"))) {
        nick = "abnormalNick"
      }
      nick = "sinaWeibo_Nick" + "," + nick

    } else {
      nick = ""
    }
    // println(nick)
    nick

  }

  def QQMusicID(uri: String, cookie: String): String = {
    var nick = ""
    val web_cookie1 = "qm_hideuin="
    val web_cookie2 = " qqmusic_uin="
    val separator = ";" //;
    val cookieElement = cookie.split(separator)
    var i = 0
    while (i < cookieElement.length) {
      var nicktemp = ""
      if ((cookieElement(i).split(web_cookie1).length > 1) && (cookieElement(i).split(web_cookie1)(1) != "")) { //early
        val nicktemp = cookieElement(i).split(web_cookie1)(1)
        if (((nicktemp.matches("^(\\s*)(\\d){5,}$")) || (nicktemp.matches("^(\\s*)o(\\d){5,}$")))) { //"^(\\s*)(\\d){5,}$"
          nick = nicktemp
        }

      }

      if ((cookieElement(i).split(web_cookie2).length > 1) && (cookieElement(i).split(web_cookie2)(1) != "")) { //later
        val nicktemp = cookieElement(i).split(web_cookie2)(1)
        if (((nicktemp.matches("^(\\s*)(\\d){5,}$")) || (nicktemp.matches("^(\\s*)o(\\d){5,}$")))) { //"^(\\s*)(\\d){5,}$"
          nick = nicktemp
        }

      }
      i += 1
    }
    if ((nick != "") && (nick != "0")) {
      if (((nick.matches("^(\\s*)(\\d){5,}$")) || (nick.matches("^(\\s*)o(\\d){5,}$")))) { //"^(\\s*)(\\d){5,}$"
        nick = nick
      } else {
        nick = "abnormalNick"
      }

      nick = "QQMusic|" + nick
    } else {
      nick = ""
    }
    nick

  }
  def QQformat(nick: String, nicktemp: String, flag: String): String = {
    var finalnick = ""
    var nickformat = ""
    var nicktemp1 = ""
    //    var laternick = nick
    // nick=
    // tmpnick = o000123  
    //
    //    var nickaa = "123456789"
    //      var nicktempbb="o000123"

    if (nicktemp.contains("o")) {
      nicktemp1 = Pattern.compile("^o0*").matcher(nicktemp).replaceAll("")
    } else {
      nicktemp1 = nicktemp
    }

    if (nick.contains("o")) {
      nickformat = Pattern.compile("^o0*").matcher(nick).replaceAll("")
    } else {
      nickformat = nick
    }
    //    println("0"+nicktemp1)

    if (nickformat.contains(nicktemp1)) {
      finalnick = nick
    } else if (nicktemp1.contains(nickformat)) {
      finalnick = nicktemp
    } else {
      finalnick = nicktemp
    }

    finalnick

  }

  def QQID(uri: String, cookie: String): String = {
    var nick = ""
    var nicktype = ""
    //    println("deal with qq")
    //        var uritest = "/xml/danmu/abulletcurtain.xml"
    //        var cookietest = "RK=ZTOm3vu4WU; pt2gguin=o0009113553; uin=o0009113553; skey=@2O6Ncv7tQ;ptisp=cnc; ptcz=f8f2e138a4a54d95e292d8c2630885b6db78ded699e41d9739e36ca91ad38ce5; pgv_info=ssid=s5094514110; pgv_pvid=4508980938; o_cookie=9113553"
    val web_uri1 = "[?&]ui[nd]=" //match "/getUpdates.do?uid=527969257"//&uin=1406401552&//?uin=

    /*lost 1%*/

    if (!(uri.contains("store_file_download")) && (!uri.matches("^/(\\?).*"))) { //always wrong

      //      println("no match")
      if ((uri.split(web_uri1).length > 1) && (uri.split(web_uri1)(1) != "")) {

        val nickString = uri.split(web_uri1)(1)
        if (nickString(0) == 'o') {
          nick = nickString.replaceAll("^o(\\d+).*", "$1")
          nicktype = "1"

        } else {
          nick = nickString.replaceAll("^(\\d+).*", "$1")
          nicktype = "2"
        }
      }

    }

    /*lost 1%*/

    if (((nick.matches("^(\\s*)(\\d){5,}$")) || (nick.matches("^(\\s*)o(\\d){5,}$")))) { //"^(\\s*)(\\d){5,}$"
      nick = nick
    } else {
      nick = ""
    }
    // }
    val web_cookie1 = "ptui_loginuin="
    val web_cookie3 = "o_cookie="
    val web_cookie8 = "qm_username="

    //          val web_cookie2 = "pt2gguin=" //4
    //    val web_cookie4 = "^(\\s*)uin=.*" //(^\\s)uin=*//6
    //    val web_cookie5 = "^(\\s*)uin_cookie=.*"
    //    val web_cookie6 = "p_o2_uin=" //p_o2_uin=870695832;  //8
    //    val web_cookie7 = "adid=" //adid=707137418

    //鈥減t2gguin鈥濄€佲€渦in鈥濄€佲€減_uin鈥?     

    val separator = ";" //;
    val cookieElement = cookie.split(separator)
    var i = 0
    while (i < cookieElement.length) {
      //      println(cookieElement(i))
      var nicktemp = ""
      if ((cookieElement(i).split(web_cookie1).length > 1) && (cookieElement(i).split(web_cookie1)(1) != "")) { //early
        val nicktemp = cookieElement(i).split(web_cookie1)(1)
        if (((nicktemp.matches("^(\\s*)(\\d){5,}$")) || (nicktemp.matches("^(\\s*)o(\\d){5,}$")))) { //"^(\\s*)(\\d){5,}$"
          nick = QQformat(nick, nicktemp, "3")
          nicktype = "3"
        }
      }
      if ((cookieElement(i).split(web_cookie3).length > 1) && (cookieElement(i).split(web_cookie3)(1) != "")) { //early
        val nicktemp = cookieElement(i).split(web_cookie3)(1)
        if (((nicktemp.matches("^(\\s*)(\\d){5,}$")) || (nicktemp.matches("^(\\s*)o(\\d){5,}$")))) { //"^(\\s*)(\\d){5,}$"

          nick = QQformat(nick, nicktemp, "5")

          nicktype = "5"
        }
      }

      if ((cookieElement(i).split(web_cookie8).length > 1) && (cookieElement(i).split(web_cookie8)(1) != "")) { //early
        val nicktemp = cookieElement(i).split(web_cookie8)(1)
        if (((nicktemp.matches("^(\\s*)(\\d){5,}$")) || (nicktemp.matches("^(\\s*)o(\\d){5,}$")))) { //"^(\\s*)(\\d){5,}$"

          nick = QQformat(nick, nicktemp, "9")

          nicktype = "0"
        }
      }
      i += 1
    }

    //    nick = "911054179"
    var p = Pattern.compile("^(\\s*)");
    var m = p.matcher(nick);
    nick = m.replaceAll("");

    //    println("nick111" + nick)

    if ((nick != "") && (nick != "0")) {

      if (nick(0) == 'o') {
        nick = nick.substring(1)
      }

      if ((nick.matches("^(\\d){5,}$"))) { //"^(\\s*)(\\d){5,}$"
        nick = nick
        var tmp = Pattern.compile("^0*").matcher(nick)
        nick = tmp.replaceAll("")

      } else {
        nick = "abnormalNick"
      }

      nick = "QQ_normal|" + nick
    } else {
      nick = ""
    }
    nick //+ "&&" + nicktype + "&&" + cookie + "&&" + uri + "&&"

  }

  def checkHash(nick: String, map: HashMap[String, String], nicktype: String): HashMap[String, String] = {

    var map_return = new HashMap[String, String]()
    map_return = map

    if (map.contains(nick)) {
      val count = map(nick) + "," + nicktype
      map_return = map_return - nick
      map_return = map_return + (nick -> count)
    } else {
      var flag = 0

      for (inHash <- map.keySet) {
        if (inHash.contains(nick)) {
          val count = map(inHash) + "," + nicktype
          map_return = map_return - inHash
          map_return = map_return + (inHash -> count)
          flag = 1
        } else if (nick.contains(inHash)) {
          val count = map(inHash) + "," + nicktype
          map_return = map_return - inHash
          map_return = map_return + (nick -> count)
          map_return
          flag = 1
        }
      }
      if (flag == 0) {
        map_return = map_return + (nick -> nicktype)
      }
    }

    map_return
  }

  def QQ_other(uri: String, cookie: String): String = {
    var nick = ""
    var nicktype = ""
    var nick_company = ""
    var nick_normal = ""

    var map = new HashMap[String, String]()
    //    println("deal with qq other")
    //        var uritest = "/xml/danmu/abulletcurtain.xml"
    //        var cookietest = "RK=ZTOm3vu4WU; pt2gguin=o0009113553; uin=o0009113553; skey=@2O6Ncv7tQ;ptisp=cnc; ptcz=f8f2e138a4a54d95e292d8c2630885b6db78ded699e41d9739e36ca91ad38ce5; pgv_info=ssid=s5094514110; pgv_pvid=4508980938; o_cookie=9113553"
    val web_uri1 = "[?&]ui[nd]=" //match "/getUpdates.do?uid=527969257"//&uin=1406401552&//?uin=

    /*lost 1%*/

    if ((!(uri.contains("store_file_download"))) && (!uri.matches("^/(\\?).*"))) { //always wrong
      //      println("no match")
      if ((uri.split(web_uri1).length > 1) && (uri.split(web_uri1)(1) != "")) {

        val nickString = uri.split(web_uri1)(1)
        if (nickString(0) == 'o') {
          nick = nickString.replaceAll("^o(\\d+).*", "$1")
          nicktype = "1"

        } else {
          nick = nickString.replaceAll("^(\\d+).*", "$1")
          nicktype = "2"
        }
      }

    }

    /*lost 1%*/

    if (((nick.matches("^(\\s*)(\\d){5,}$")) || (nick.matches("^(\\s*)o(\\d){5,}$")))) { //"^(\\s*)(\\d){5,}$"
      nick = nick
      map = checkHash(nick, map, "1")
    } else {
      nick = ""
    }
    // }
    //    val web_cookie1 = "ptui_loginuin="
    val web_cookie2 = "pt2gguin=" //4
    val web_cookie4 = "^(\\s*)uin=.*" //(^\\s)uin=*//6
    val web_cookie5 = "^(\\s*)uin_cookie=.*"
    val web_cookie6 = "p_o2_uin=" //p_o2_uin=870695832;  //8
    val web_cookie7 = "adid=" //adid=707137418

    val separator = ";" //;
    val cookieElement = cookie.split(separator)
    var i = 0
    while (i < cookieElement.length) {
      //      println(cookieElement(i))
      var nicktemp = ""

      if ((cookieElement(i).split(web_cookie2).length > 1) && (cookieElement(i).split(web_cookie2)(1) != "")) { //early
        val nicktemp = cookieElement(i).split(web_cookie2)(1)
        if (((nicktemp.matches("^(\\s*)(\\d){5,}$")) || (nicktemp.matches("^(\\s*)o(\\d){5,}$")))) { //"^(\\s*)(\\d){5,}$"
          nick = QQformat(nick, nicktemp, "4")
          map = checkHash(nick, map, "4")
          nicktype = "4"
        }

      }

      if (cookieElement(i).matches(web_cookie4)) {
        if ((cookieElement(i).split("uin=").length > 1) && (cookieElement(i).split("uin=")(1) != "")) {
          val nickString = cookieElement(i).split("uin=")(1)

          var nicktemp = ""
          if (nickString(0) == 'o') {
            nicktemp = nickString.replaceAll("^o(\\d+).*", "$1")
          } else {
            nicktemp = nickString.replaceAll("^(\\d+).*", "$1")
          }

          nick = QQformat(nick, nicktemp, "6")
          map = checkHash(nick, map, "6")

          nicktype = "6"
        }
      }
      if (cookieElement(i).matches(web_cookie5)) {
        if ((cookieElement(i).split("uin_cookie=").length > 1) && (cookieElement(i).split("uin_cookie=")(1) != "")) {
          val nickString = cookieElement(i).split("uin_cookie=")(1)

          //          val nicktemp = nickString.replaceAll("^(\\d+).*", "$1")

          var nicktemp = ""
          if (nickString(0) == 'o') {
            nicktemp = nickString.replaceAll("^o(\\d+).*", "$1")
          } else {
            nicktemp = nickString.replaceAll("^(\\d+).*", "$1")
          }
          if (((nicktemp.matches("^(\\s*)(\\d){5,}$")) || (nicktemp.matches("^(\\s*)o(\\d){5,}$")))) { //"^(\\s*)(\\d){5,}$"
            nick = QQformat(nick, nicktemp, "7")
            map = checkHash(nick, map, "7")
            nicktype = "7"
          }
        }
      }
      if ((cookieElement(i).split(web_cookie6).length > 1) && (cookieElement(i).split(web_cookie6)(1) != "")) { //early
        val nicktemp = cookieElement(i).split(web_cookie6)(1)
        if (((nicktemp.matches("^(\\s*)(\\d){5,}$")) || (nicktemp.matches("^(\\s*)o(\\d){5,}$")))) { //"^(\\s*)(\\d){5,}$"

          nick = QQformat(nick, nicktemp, "8")
          map = checkHash(nick, map, "8")

          nicktype = "8"
        }
      }
      if ((cookieElement(i).split(web_cookie7).length > 1) && (cookieElement(i).split(web_cookie7)(1) != "")) { //early
        val nicktemp = cookieElement(i).split(web_cookie7)(1)
        if (((nicktemp.matches("^(\\s*)(\\d){5,}$")) || (nicktemp.matches("^(\\s*)o(\\d){5,}$")))) { //"^(\\s*)(\\d){5,}$"

          nick = QQformat(nick, nicktemp, "9")
          map = checkHash(nick, map, "9")

          nicktype = "9"
        }
      }
      i += 1
    }

    if (map.size == 2) {

      for (nickxx <- map.keySet) {
        var nick = nickxx

        var p = Pattern.compile("^(\\s*)")
        var m = p.matcher(nick)
        nick = m.replaceAll("")
        if ((nick != "") && (nick != "0")) {

          if (nick(0) == 'o') {
            nick = nick.substring(1)
          }

          if ((nick.matches("^(\\d){5,}$"))) { //"^(\\s*)(\\d){5,}$"
            nick = nick
            var tmp = Pattern.compile("^0*").matcher(nick)
            nick = tmp.replaceAll("")

          } else {
            nick = "abnormalNick"
          }
          if ((map(nickxx).contains("4"))) { // || (map(nickxx).contains("6")) || (map(nickxx).contains("8"))

            nick_company = "QQ_company|" + nick
          } else if ((map(nickxx).contains("7"))) {
            nick_normal = "QQ_normal|" + nick
          }
          //          else {
          //            nick_normal = nick + "|QQ_other"//may be wrong in logic
          //          }
        } else {
          nick = ""
        }

      }
    } else {

      for (nickxx <- map.keySet) {

        var nick = nickxx
        var p = Pattern.compile("^(\\s*)")
        var m = p.matcher(nick)
        nick = m.replaceAll("")
        if ((nick != "") && (nick != "0")) {

          if (nick(0) == 'o') {
            nick = nick.substring(1)
          }

          if ((nick.matches("^(\\d){5,}$"))) { //"^(\\s*)(\\d){5,}$"
            nick = nick
            var tmp = Pattern.compile("^0*").matcher(nick)
            nick = tmp.replaceAll("")

          } else {
            nick = "abnormalNick"
          }

          nick_normal = "QQ_normal|" + nick
          nick_company = ""
        }
      }

    }

    //    println("end")

    //    nick = "911054179"

    //    println("nick111" + nick)
    //        println(nick_normal + "&&" + nick_company)

    nick_normal + "," + nick_company //+ "&&" + nicktype + "&&" + cookie + "&&" + uri + "&&"

  }

  def QQ_Company(uri: String, cookie: String): String = {
    var nick = ""
    var nicktype = ""
    //    println("deal with qq")
    //        var uritest = "/xml/danmu/abulletcurtain.xml"
    //        var cookietest = "RK=ZTOm3vu4WU; pt2gguin=o0009113553; uin=o0009113553; skey=@2O6Ncv7tQ;ptisp=cnc; ptcz=f8f2e138a4a54d95e292d8c2630885b6db78ded699e41d9739e36ca91ad38ce5; pgv_info=ssid=s5094514110; pgv_pvid=4508980938; o_cookie=9113553"
    val web_uri1 = "[?&]ui[nd]=" //match "/getUpdates.do?uid=527969257"//&uin=1406401552&//?uin=
    val web_uri2 = "clientuin="

    /*lost 1%*/

    if ((uri.split(web_uri1).length > 1) && (uri.split(web_uri1)(1) != "")) {

      val nickString = uri.split(web_uri1)(1)
      if (nickString(0) == 'o') {
        nick = nickString.replaceAll("^o(\\d+).*", "$1")
        nicktype = "1"

      } else {
        nick = nickString.replaceAll("^(\\d+).*", "$1")
        nicktype = "2"
      }
    } else if ((uri.split(web_uri2).length > 1) && (uri.split(web_uri2)(1) != "")) {

      val nickString = uri.split(web_uri2)(1)
      if (nickString(0) == 'o') {
        nick = nickString.replaceAll("^o(\\d+).*", "$1")
        nicktype = "1"

      } else {
        nick = nickString.replaceAll("^(\\d+).*", "$1")
        nicktype = "2"
      }
    }

    /*lost 1%*/

    if (((nick.matches("^(\\s*)(\\d){5,}$")) || (nick.matches("^(\\s*)o(\\d){5,}$")))) { //"^(\\s*)(\\d){5,}$"
      nick = nick
    } else {
      nick = ""
    }

    val web_cookie3 = "p_uin"
    val web_cookie2 = "super_uin"

    val web_cookie1 = "pt2gguin="
    val web_cookie4 = "^(\\s*)uin=.*" //(^\\s)uin=*
    //    val web_cookie5 = "^(\\s*)uin_cookie=.*"
    val web_cookie6 = "p_o2_uin=" //p_o2_uin=870695832;  
    val web_cookie7 = "adid=" //adid=707137418

    val separator = ";" //;
    val cookieElement = cookie.split(separator)
    var i = 0
    while (i < cookieElement.length) {
      //      println(cookieElement(i))
      var nicktemp = ""

      if ((cookieElement(i).split(web_cookie2).length > 1) && (cookieElement(i).split(web_cookie2)(1) != "")) { //early
        val nicktemp = cookieElement(i).split(web_cookie2)(1)
        if (((nicktemp.matches("^(\\s*)(\\d){5,}$")) || (nicktemp.matches("^(\\s*)o(\\d){5,}$")))) { //"^(\\s*)(\\d){5,}$"
          nick = QQformat(nick, nicktemp, "0")
          nicktype = "4"
        }

      }
      if ((cookieElement(i).split(web_cookie3).length > 1) && (cookieElement(i).split(web_cookie3)(1) != "")) { //early
        val nicktemp = cookieElement(i).split(web_cookie3)(1)
        if (((nicktemp.matches("^(\\s*)(\\d){5,}$")) || (nicktemp.matches("^(\\s*)o(\\d){5,}$")))) { //"^(\\s*)(\\d){5,}$"

          nick = QQformat(nick, nicktemp, "0")

          nicktype = "5"
        }
      }

      if ((cookieElement(i).split(web_cookie1).length > 1) && (cookieElement(i).split(web_cookie1)(1) != "")) { //early
        val nicktemp = cookieElement(i).split(web_cookie1)(1)
        if (((nicktemp.matches("^(\\s*)(\\d){5,}$")) || (nicktemp.matches("^(\\s*)o(\\d){5,}$")))) { //"^(\\s*)(\\d){5,}$"
          nick = QQformat(nick, nicktemp, "4")
          nicktype = "4"
        }

      }

      if (cookieElement(i).matches(web_cookie4)) {
        if ((cookieElement(i).split("uin=").length > 1) && (cookieElement(i).split("uin=")(1) != "")) {
          val nickString = cookieElement(i).split("uin=")(1)

          var nicktemp = ""
          if (nickString(0) == 'o') {
            nicktemp = nickString.replaceAll("^o(\\d+).*", "$1")
          } else {
            nicktemp = nickString.replaceAll("^(\\d+).*", "$1")
          }

          nick = QQformat(nick, nicktemp, "6")

          nicktype = "6"
        }
      }
      //      if (cookieElement(i).matches(web_cookie5)) {
      //        if ((cookieElement(i).split("uin_cookie=").length > 1) && (cookieElement(i).split("uin_cookie=")(1) != "")) {
      //          val nickString = cookieElement(i).split("uin_cookie=")(1)
      //
      //          //          val nicktemp = nickString.replaceAll("^(\\d+).*", "$1")
      //
      //          var nicktemp = ""
      //          if (nickString(0) == 'o') {
      //            nicktemp = nickString.replaceAll("^o(\\d+).*", "$1")
      //          } else {
      //            nicktemp = nickString.replaceAll("^(\\d+).*", "$1")
      //          }
      //          if (((nicktemp.matches("^(\\s*)(\\d){5,}$")) || (nicktemp.matches("^(\\s*)o(\\d){5,}$")))) { //"^(\\s*)(\\d){5,}$"
      //            nick = QQformat(nick, nicktemp, "7")
      //            nicktype = "7"
      //          }
      //        }
      //      }
      if ((cookieElement(i).split(web_cookie6).length > 1) && (cookieElement(i).split(web_cookie6)(1) != "")) { //early
        val nicktemp = cookieElement(i).split(web_cookie6)(1)
        if (((nicktemp.matches("^(\\s*)(\\d){5,}$")) || (nicktemp.matches("^(\\s*)o(\\d){5,}$")))) { //"^(\\s*)(\\d){5,}$"

          nick = QQformat(nick, nicktemp, "8")

          nicktype = "8"

        }
      }
      if ((cookieElement(i).split(web_cookie7).length > 1) && (cookieElement(i).split(web_cookie7)(1) != "")) { //early
        val nicktemp = cookieElement(i).split(web_cookie7)(1)
        if (((nicktemp.matches("^(\\s*)(\\d){5,}$")) || (nicktemp.matches("^(\\s*)o(\\d){5,}$")))) { //"^(\\s*)(\\d){5,}$"

          nick = QQformat(nick, nicktemp, "9")

          nicktype = "9"
        }
      }

      i += 1
    }

    //    nick = "911054179"
    var p = Pattern.compile("^(\\s*)");
    var m = p.matcher(nick);
    nick = m.replaceAll("");

    //    println("nick111" + nick)

    if ((nick != "") && (nick != "0")) {

      if (nick(0) == 'o') {
        nick = nick.substring(1)
      }

      if ((nick.matches("^(\\d){5,}$"))) { //"^(\\s*)(\\d){5,}$"
        nick = nick

        var tmp = Pattern.compile("^0*").matcher(nick)
        nick = tmp.replaceAll("")

      } else {
        nick = "abnormalNick"
      }

      nick = "QQ_company" + "," + nick
    } else {
      nick = ""
    }
    //            println(nick)
    //    nick + "&&" + nicktype
    nick //+ "&&" + nicktype + "&&" + cookie + "&&" + uri + "&&"

  }

  def TimeStamp2Date(timestampString: String, formats: String): Timestamp = {
    val timestamp: Long = timestampString.toLong

    val date: String = new java.text.SimpleDateFormat(formats).format(new java.util.Date(timestamp))

    val CreateDate: Timestamp = Timestamp.valueOf(date);
    CreateDate
  }

  def main(args: Array[String]) = {

    if (args.length != 2) {
      sys.error("Usage:<input><output>")
    }

    val Array(input, output) = args

    var outputString = ""

    val conf = new SparkConf
    conf.setAppName("virtual-identity-extraction")//.setMaster("local")
    val sc = new SparkContext(conf)

    var rowkey = ""
    var time = ""
    var userip = ""
    var host = ""
    var uri = ""
    var cookie = ""
    var useragent = ""
    var nick = ""
    var nick1 = ""
    var nick2 = ""
    var nick3 = ""

    var pid_type = ""

    var nick_type = ""
    var nick1_type = ""
    var nick2_type = ""
    var nick3_type = ""
    //    var serialNum = 0
    val timeAndID = sc.textFile(input).map(_.split("\\|"))
      .map(x => {
        //    val y :String = URLEncoder.encode(x,"UTF8")
        val length = x.length

          time = x(LOGTIMESTAMP_INDEX_14)
          userip = x(USERIPv4_INDEX_14)
          host = x(HOST_14)
          uri = x(URI_INDEX_14)
          cookie = x(COOKIE_INDEX_14)
          useragent=x(USERAGENT_14)

        nick = ""
        nick_type = ""
        nick1 = ""
        nick1_type = ""
        nick2 = ""
        nick2_type = ""
        nick3 = ""
        nick3_type = ""

        /*deal with taobao*/

        //                host = "btrace.video.jd.com"
        // alpin=sddy0002%40163.com; 
        // pin=ceshi;
        //
//        cookie = "alpin=zanezanefine; TrackID=1InnfgQSBV7smDIyzYeEdpG3PgJ-nIx5wdpFcCWtsib83ALE4Z2gwcVrEnnVbaUWhZqPocWHM6Lk7BXi4U751cmw5TJiSp4iv6B0AitQrWKFMVHbleOW3Xh8L5Wduow1c; __jda=122270672.795407632.1384421787.1450478312.1452041508.177; __jdb=122270672.1.795407632 177.1452041508; __jdc=122270672; __jdu=795407632; __jdv=122270672 media - cpc media_8_58871498_s2291252f04b3aeca346.43399213; cn=20; ipLoc-djd=1-72-2799-0; ipLocation=%u5317%u4EAC; mba_muid=1431785899229-1a512622903cdab2cf; pinId=3ctxfw7HWoguZnM_ZPwqzA; thor=0E051E2ECE85E625072D0C783328892B4EE1F63E910E01601C375E42B633F370B4851B04F713473FBDE254B4BDA59556FF5D36A0CB9F512FCF5089A763E3C117C7B05F7FF17DAA81345BE37A5B33557C8B53B1375459BF4F3A35BB36C075FCFF55853B6E8C7F21779ED1271FC8E7FF2A1802FCDA92CA9BF34E62C238A9E731BACBE80DB60E7E53F9DAED72DEAE881AE2; user-key=dc0ec9c0-3615-401b-9d0e-df398b7a531d"
        //                  "user-key=46adac93-ef69-439d-872d-646528f37617; mp=song-rn; _tp=SbhyzYMnpfPYdpAhC92D2w%3D%3D; _pst=song-rn; TrackID=1EOhGfrb-yOu6ThAVReAUk0EaZMO6SHKyzcQ_hoEit3iSh9vinvjHL3weOWA3yeHDXinXFA4z4Ypc6itE5gI96Q; pinId=HsJDvXRJvZo; unick=song-rn; pin=song-rn; alpin=song-rn; __utmz=122270672.1450278938.1.1.utmcsr=trade.jd.com utmccn=(referral) utmcmd=referral utmcct=/shopping/order/getOrderInfo.action; sec=1451286178730; unpl=V2_ZzNtbUdQERd1WE5WeEteDWIAFQhKBUtHdVhGV3IdXQ1kURMJclRCFXIUR1RnGFQUZwsZXERcQhVFCHZXfBpaAmEBFl5yBBNNIEwEACtaDlwJARJYQ19DHHAORl1LKVwMVwMTbUJQQxJwDEJUeilsAlczIlhFUEcdfDhHZHo%3d; mt_subsite=  1111%2C1452337621; mt_xid=V2_52007VwMVUlpdVl4fSClaAzNRR1BdW05cHEtMQAA1VxFODlhTCANLTAhWNVdHAVleUVovShhcAnsCEk5cUENaF0IZXg5lACJQbVtiWR5PEVkAVwAVWw%3D%3D; cn=0; ipLocation=%u5317%u4EAC; areaId=1; ipLoc-djd=1-2800-2848-0; thor=F849691693F71AF8BFA9EA9BE034756A8F360CA0B3BC1CA20F9EB1168036B7491629BBD460B269C409DBC785F62DFA17E323594DF7156976D1C964"
//        uri = ""
        //                  "/new/helloService.ashx?m=ls&callback=jQuery16209334850572049618_1452483315630&_=1452483319926"
//        host = "gm.jd.com"
        if (host.contains("jd.com")) {
          //         println("jd")
          nick = JD(uri, cookie)
//          println(nick)
          if (nick.contains(",")) {
            if ((nick.split(",").length > 0) && (nick.split(",")(0) != "")) {
              nick1 = nick.split(",")(0)
//              println("nick1" + nick1)
            }
            if ((nick.split(",").length > 1) && (nick.split(",")(1) != "")) {
              nick2 = nick.split(",")(1)
//              println("nick2" + nick2)
            }
            nick = ""
            nick_type = ""
            if ((nick2.matches("^1[3|4|5|8](\\d){9}$"))) {

              //                println("nick2:")
              nick2 = nick2
              nick2_type = "JD_MSISDN"
              if (nick1 != "") {
                nick1 = nick1
                nick1_type = "JD_Nick"
              }

            } else if (nick2.matches("[_a-zA-Z\\d\\-\\./]+%40[_a-zA-Z\\d\\-]+(\\.[_a-zA-Z\\d\\-]+)+")) { //if truncation?
              nick2 = nick2
              nick2_type = "JD_Email"
              if (nick1 != "") {
                nick1 = nick1
                nick1_type = "JD_Nick"
              }

            } else {
              if (nick1 != "") {
                nick1 = nick1
                nick1_type = "JD_Nick"

//                println("nick +type " + nick1 + nick1_type)
              }
              if (nick2 != "") {
                nick2 = nick2
                nick2_type = "JD_Nick"
              }
            }
          }
          //                    println(nick + "+" + nick1 + "+" + nick2)

        }

        if ((nick == "") && ((host.contains("tmall.com")) || (host.contains("mmstat.com")) || (host.contains("alisoft.com")) || (host.contains("taobao.com")) || (host.contains("hupan.com")))) {
          //           println("tmall")

          //          var nickString = ""
          //          var nicktype = ""
          nick = taobao(uri, cookie)

          if (nick.contains(",")) { //change to duo output
            if (nick.split(",")(0) != "") {
              nick1 = nick.split(",")(0)
            }
            if (nick.split(",")(1) != "") {
              nick2 = nick.split(",")(1)
            }
            nick = ""
            nick_type = ""

            if (nick1.matches("^1[3|4|5|8](\\d){9}$") || (nick2.matches("^1[3|4|5|8](\\d){9}$"))) {
              if (nick1.matches("^1[3|4|5|8](\\d){9}$")) {
                //                println("nick1:")
                nick1 = nick1
                nick1_type = "taobao_MSISDN"
                nick2 = nick2
                nick2_type = "taobao_Nick"
              } else {
                //                println("nick2:")
                nick2 = nick2
                nick2_type = "taobao_MSISDN"
                nick1 = nick1
                nick1_type = "taobao_Nick"
              }
            }
          } else if (nick != "") {
            nick = nick
            nick_type = "taobao_Nick"
          }

        }

        //        println(nick1 + "," + nick2)

        ////iask?*/
        if ((nick == "") && ((host.contains("sina")) || (host.contains("weibo")))) { //contains "weibo"

          nick1 = SianWeiboNum(uri, cookie) // nick = "sinaWeibo_ID" + "," + nick

          if (nick1.contains("sinaWeibo_ID")) {

            if (nick1.split(",").length > 1) {
              nick1_type = "sinaWeibo_ID"
              nick1 = nick1.split(",")(1)
            }
          }

          nick2 = SianWeiboRegisterNum(uri, cookie) //      nick = "sinaWeibo_Mail" + "," + nick
          if (nick2.contains("sinaWeibo_Mail")) {

            if (nick2.split(",").length > 1) {
              nick2_type = "sinaWeibo_Mail"
              nick2 = nick2.split(",")(1)
              //              println("nick2="+ nick2)
              //              println("nick1="+ nick1)
            }
          }

          //    nick3 = SianWeibonick(uri, cookie)

        }
        //
        if ((nick == "") && (host.contains("music.qq.com"))) {
          //           println("qqmusic")
          nick = QQMusicID(uri, cookie) // nick = "QQMusic|" + nick
          if (nick.contains("QQMusic")) {

            if (nick.split("\\|").length > 1) {
              nick_type = "QQMusic"
              nick = nick.split("\\|")(1)
            }
          }

        }

        if ((nick == "") && (host.contains("qq.com"))) {

          nick = QQMusicID(uri, cookie)
          if (nick.contains("QQMusic")) {

            if (nick.split("\\|").length > 1) {
              nick_type = "QQMusic"
              nick = nick.split("\\|")(1)
            }
          }
          if ((nick == "") && (host.contains("weixin") != true) && ((host.contains("game")) != true) && ((host.contains("video")) != true)) { //video unstudy

            if ((uri.contains("hrtx")) || (cookie.contains("hrtx"))) { // hrtx
              nick = QQ_Company(uri, cookie) //      nick = "QQ_company" + "," + nick
              if (nick.contains("QQ_company")) {
                if (nick.split(",").length > 1) {
                  nick_type = "QQ_company"
                  nick = nick.split(",")(1)
                }
              }
            } else {
              nick = QQID(uri, cookie) //      nick = "QQ_normal|" + nick
              if (nick.contains("QQ_normal")) {
                if (nick.split("\\|").length > 1) {
                  nick_type = "QQ_normal"
                  nick = nick.split("\\|")(1)
                }
              }
            }
            if (nick == "") {
              nick = QQ_other(uri, cookie)
              if (nick.contains(",")) {
                if ((nick.split(",").length > 0) && (nick.split(",")(0) != "")) {
                  nick1 = nick.split(",")(0) // nick_normal + "," + nick_company 
                  if (nick1.contains("QQ_normal")) {
                    if (nick1.split("\\|").length > 1) {
                      nick1_type = "QQ_normal"
                      nick1 = nick1.split("\\|")(1)
                    }
                  }
                }
                nick = ""
                nick_type = ""

              }

            }

          }

        }
//        nick = "aaa"
        if (nick != "") { //single output
          outputString = time + "|" + pid_type + "|" + userip + "|" + nick_type + "|" + nick + "|" + host + "|" + useragent + "|" + uri
//          outputString = "aaaaaaaaabbbbbbb"
        } 
        else { //multiple output
          if (nick1 != "") {
            outputString = time + "|" + pid_type + "|" + userip + "|" + nick1_type + "|" + nick1 + "|" + host + "|" + useragent + "|" + uri
          } else {
            outputString = ""
          }
          if (nick2 != "") {
            if ((outputString != "")) {
              outputString = outputString + "\n" + time + "|" + pid_type + "|" + userip + "|" + nick2_type + "|" + nick2 + "|" + host + "|" + useragent + "|" + uri
            } else if ((outputString == "")) {
              outputString = outputString + time + "|" + pid_type + "|" + userip + "|" + nick2_type + "|" + nick2 + "|" + host + "|" + useragent + "|" + uri
            }
          }
          if ((nick1 != "") && (nick2 != "")) {
            if ((outputString != "")) {

              outputString = outputString + "\n" + time + "|" + nick1_type + "|" + nick1 + "|" + nick2_type + "|" + nick2 + "|" + host + "|" + useragent + "|" + uri
            } else if ((outputString == "")) {

              outputString = outputString + time + "|" + nick1_type + "|" + nick1 + "|" + nick2_type + "|" + nick2 + "|" + host + "|" + useragent + "|" + uri
            }

          }
        }
        outputString
//        length
      })
      .saveAsTextFile(output)
  }
}