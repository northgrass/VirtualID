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

object PhoneMatch {

  def main(args: Array[String]) {

    if (args.length != 2) {
      sys.error("Usage:<input><output>")
    }

    val Array(input, output) = args

    val sparkConf = new SparkConf().setAppName("PhoneMatch")//.setMaster("local")

    sparkConf.set("spark.akka.frameSize", "100")
    val sc = new SparkContext(sparkConf)


    val oriRatings1 = sc.textFile(input).repartition(30).map(_.split(",")).filter(_.length > 2).map(fields => {

      val id1 = fields(0).trim()
      val id2 = fields(1).trim()

      (id1,id2)
      }).distinct.persist(StorageLevel.DISK_ONLY)
      
      
      val oriRatings2=oriRatings1.map(x=>(x._2,x._1))
      
      val oriRatings=oriRatings1.union(oriRatings2).map(x => {
        val nick1 = x._1
        val nick2 = x._2
        val type1 = nick1.split("\\|")(0).trim()
        val phone1 = nick1.split("\\|")(1).trim()
        val name2 = nick2.split("\\|")(1).trim()
        var outputStrig = ""
        if((type1.contains("QQ")&&nick2.contains("taobao_Nick"))||(type1.contains("QQ")&&nick2.contains("JD_Nick"))){
          if(name2.contains(phone1)){
            outputStrig = nick1 + "," + nick2
          }
        }
        
        outputStrig
      })
    
         val out = oriRatings.saveAsTextFile(output)

    
  }

}

