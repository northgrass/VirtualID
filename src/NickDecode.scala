import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Random
import java.net.URLEncoder
import java.net.URLDecoder
import org.apache.hadoop.hdfs.server.balancer.Balancer.Matcher
import java.util.regex.Pattern
import scala.collection.immutable
import collection.immutable.HashMap
import java.nio.charset.Charset;

import java.sql.Timestamp
import java.util.Calendar
import scala.util.Properties
import java.util.Properties
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
object NickDecode {

  def decodeUnicode(str: String): String = {
    var set = Charset.forName("UTF-16");
    val p = Pattern.compile("\\\\u([0-9a-fA-F]{4})");
    var m = p.matcher(str);
    var start = 0;
    var start2 = 0;
    var sb = new StringBuffer();
    while (m.find(start)) {
      start2 = m.start();
      if (start2 > start) {
        var seg = str.substring(start, start2);
        sb.append(seg);
      }
      var code = m.group(1);
      var i = Integer.valueOf(code, 16);
      var bb = new Array[Byte](4);
      bb(0) = ((i >> 8) & 0xFF).toByte
      bb(1) = (i & 0xFF).toByte
      var b = ByteBuffer.wrap(bb);
      sb.append(String.valueOf(set.decode(b)).trim());
      start = m.end();
    }
    start2 = str.length();
    if (start2 > start) {
      var seg = str.substring(start, start2);
      sb.append(seg);
    }
    return sb.toString();
  }

  def main(args: Array[String]) {

    if (args.length != 2) {
      sys.error("Usage:<input><output>")
    }

    val Array(input, output) = args

    val sparkConf = new SparkConf().setAppName(" SID Similar based on single IP") //.setMaster("local")

    sparkConf.set("spark.akka.frameSize", "100")
    val sc = new SparkContext(sparkConf)

    var userid = ""
    var itemid = ""
    // extract (userid, itemid, rating) from ratings data
    //(2016-01-05 17:00:00,111.28.33.16,QQ_normal413928314,5)
    val oriRatings = sc.textFile(input).repartition(30).map(_.split(",")).filter(_.length == 2).map(fields => {
    	
      var s_id = fields(1).replace("cntaobao", "")
      val type1 = fields(1).split("\\|")(0)
      val name1 = fields(1).split("\\|")(1).replace("cntaobao", "")
      var name1Decode = name1
      try {
        if (type1.contains("taobao_Nick")) {
          if (!name1.matches(".*%.{0,1}$") //末尾的一组编码如果残缺，不会进行解码，否则会抛异常
            && !name1.contains("%u") && !name1.contains("锟") && !name1.contains("拷")) {
            if (name1.contains("%")) {
              name1Decode = URLDecoder.decode(name1, "utf-8"); //含%的就尝试解码
              if (name1Decode.contains("�")) {
                name1Decode = URLDecoder.decode(name1, "gb2312"); //可能是解码方式错误出现乱码，改变解码方式
              }
              if (name1Decode.contains("%")) {
                name1Decode = URLDecoder.decode(name1, "utf-8"); //如果是经过两次编码，则需要再解一次码
              }
              if (name1Decode.contains("\\u")) {
                name1Decode = decodeUnicode(name1Decode); //解码结果是Unicode码，需要用Unicode解码方法解码
              }
            }
          }
        }
        if (type1.contains("JD_Nick")) {
          if (!name1.matches(".*%.{0,1}$")) {
            if (name1.contains("%u")) {
              if (name1.matches("(%u\\w{4})+")) {
                name1Decode = name1.replaceAll("%u(\\w{2})(\\w{2})", "%$1%$2"); //将字符串中的"u"去掉，并在每四位中的第二位后面插入%
                name1Decode = URLDecoder.decode(name1Decode, "utf-8");
              }
            } else if (name1.contains("%")) {
              name1Decode = URLDecoder.decode(name1, "utf-8"); //可直接解码的情况
            }
          }
        }
        if (!name1.equals(name1Decode)) {
          s_id = s_id.replace(name1, name1Decode); //用解码结果替换原用户名，下同
        }

      } catch {
        case ex: FileNotFoundException => {
          println("Missing file exception")
        }
        case ex: IllegalArgumentException => {
          println("badbadbad")
        }
      }

      (fields(0), s_id)
    }).filter(!_._2.contains("%")).distinct.saveAsTextFile(output)

  }

}