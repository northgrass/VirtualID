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
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

object extractTaobao {

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
  var nicktype = ""

  //18
  def taobao(uri: String, cookie: String): String = {
    //    println("deal with taobao")

    var nick = ""
    nicktype = ""

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

    var outputfile = "outputuri" + nicktype.toString()

    nick
  }

//  class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
//    override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
//      key.asInstanceOf[String]
//  }

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
        useragent = x(USERAGENT_14)

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
        var nick222 = ""

        if ((nick == "") && ((host.contains("tmall.com")) || (host.contains("mmstat.com")) || (host.contains("alisoft.com")) || (host.contains("taobao.com")) || (host.contains("hupan.com")))) {
          //           println("tmall")

          //          var nickString = ""
          //          var nicktype = ""
          nick = taobao(uri, cookie)
          nick222 = nick

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

        //

        //        nick = "aaa"
        if (nick != "") { //single output
          outputString = pid_type + "|" + userip + "|" + nick_type + "|" + nick + "|" + uri
          //          outputString = "aaaaaaaaabbbbbbb"
        } else { //multiple output
          if (nick1 != "") {
            outputString = pid_type + "|" + userip + "|" + nick1_type + "|" + nick1 + "|" + uri
          } else {
            outputString = ""
          }
          if (nick2 != "") {
            if ((outputString != "")) {
              outputString = outputString + "," +  pid_type + "|" + userip + "|" + nick2_type + "|" + nick2 + "|" + uri
            } else if ((outputString == "")) {
              outputString = outputString + pid_type + "|" + userip + "|" + nick2_type + "|" + nick2 + "|" + uri
            }
          }
          if ((nick1 != "") && (nick2 != "")) {
            if ((outputString != "")) {

              outputString = outputString + "," + nick1_type + "|" + nick1 + "|" + nick2_type + "|" + nick2 + "|" + uri
            } else if ((outputString == "")) {

              outputString = outputString + nick1_type + "|" + nick1 + "|" + nick2_type + "|" + nick2 + "|" + uri
            }

          }
        }
        

        (nicktype,outputString)
        //        length
      }).filter(_._2 != "").filter(_._1 == "7")
//      .repartition(1)
      .saveAsTextFile(output)

    // "outputuri"+nicktype.toString()
  }
}