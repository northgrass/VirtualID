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

object RemainUserIP {

  def main(args: Array[String]) {

    if (args.length != 3) {
      sys.error("Usage:<input1><input2><output>")
    }

    val Array(input1, input2, output) = args

    val sparkConf = new SparkConf().setAppName(" RemainUserIP Match")// .setMaster("local")

    sparkConf.set("spark.akka.frameSize", "200")
    sparkConf.set("spark.akka.askTimeout", "600")
    sparkConf.set("spark.akka.timeout", "600")

    // akka.pattern.AskTimeoutException: Timed out

    sparkConf.set("spark.shuffle.file.buffer.kb", "500")
    sparkConf.set("spark.kryoserializer.buffer.mb", "512")

    sparkConf.set("spark.core.connection.auth.wait.timeout", "300")
    sparkConf.set("spark.shuffle.consolidateFiles ", "true")
    val sc = new SparkContext(sparkConf)

    var userid = ""
    var itemid = ""

    val oriRatings = sc.textFile(input1).map(_.split(",")).filter(_.length == 1).map(fields => {

      val usernick = fields(0).split("\t")(0)

      (usernick,1L)
    })

    val oriRatings1 = sc.textFile(input2).map(_.split(",")).filter(_.length == 3).map(fields => {

      val timeIndex = fields(0)
      val ip = fields(1)
      val usernick = fields(2).split("\\)")(0)
      val info = timeIndex+","+ip

      (usernick, info)
    })

    val merge = oriRatings.join(oriRatings1).map {
      case (id, (count, info)) => (id, info)
    }
    
     val out = merge.saveAsTextFile(output)

  }

}
