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
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.catalyst.plans.logical.Repartition

object RemainUserOnlinFlag {

  def main(args: Array[String]) {

    if (args.length != 2) {
      sys.error("Usage:<input><output>")
    }

    val Array(input, output) = args

    val sparkConf = new SparkConf().setAppName(" RemainUserIP Online flag")//.setMaster("local")

    sparkConf.set("spark.akka.frameSize", "100")
    val sc = new SparkContext(sparkConf)

    var userid = ""
    var itemid = ""

    val oriRatings = sc.textFile(input).repartition(30).map(_.split(",")).filter(_.length == 4).map(fields => {

      val basenick = fields(0).split("\\(")(1)
      val linknick = fields(1)
      val timeIndex = fields(2)

      (basenick,linknick,timeIndex)}).distinct.persist(StorageLevel.DISK_ONLY)
      
      
      val oriRatings1=oriRatings.map(x=>((x._1+","+x._2+","+x._3),1L))
      
      val oriRatings2=oriRatings.map(x=>((x._2+","+x._1+","+x._3),1L))

      
      
      
      
      
      
      
      
      
//      val key = basenick + "," + linknick + "," + timeIndex
//      //      (basenick,linknick,timeIndex,1L)
//      //   val baseuserflag = 1
//
//      (key, 1L)
//
//    }).distinct
//    val oriRatings2 = sc.textFile(input).map(_.split(",")).filter(_.length == 4).map(fields => {
//
//      val basenick = fields(0).split("\\(")(1)
//      val linknick = fields(1)
//      val timeIndex = fields(2)
//
//      //(linknick,basenick,timeIndex,1L)
//      val key = linknick + "," + basenick + "," + timeIndex
//      (key, 1L)
//
//    }).distinct

    val merge = oriRatings2.join(oriRatings1).map {
//      case (id, (baseflag, linkflag)) => (id,baseflag, linkflag)
      x=>x._1+"\t"+x._2._1+"\t"+x._2._2
    }
    
         val out = merge.saveAsTextFile(output)

    
  }

}

//可以在本地测试一下这种方法
