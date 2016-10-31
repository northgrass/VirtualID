import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Random
import java.net.URLEncoder
import java.net.URLDecoder
import org.apache.hadoop.hdfs.server.balancer.Balancer.Matcher
import java.util.regex.Pattern
import java.sql.Timestamp
import java.util.Calendar
import scala.util.Properties
import java.util.Properties
import java.util.HashMap
import org.apache.velocity.runtime.directive.Foreach
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.catalyst.plans.logical.Repartition


object PreciseDelete {

  def main(args: Array[String]) {

    if (args.length != 3) {
      sys.error("Usage:<input1><input2><output>")
    }

    val Array(input1, input2, output) = args

    val sparkConf = new SparkConf().setAppName("Filter") //.setMaster("local")

    sparkConf.set("spark.akka.frameSize", "100")
    val sc = new SparkContext(sparkConf)

    
    val hsmap = sc.textFile(input1).repartition(30).map(_.split(",")).filter(_.length == 2)
    .map(fields => {
      val nick1 = fields(0).replace("(", "")
      val nick1type = nick1.split("\\|")(0).trim()
      val nick1id = nick1.split("\\|")(1).trim()
      val nick2 = fields(1).replace(")", "")
      val nick2type = nick2.split("\\|")(0).trim()
      val nick2id = nick2.split("\\|")(1).trim()
      
      val map = new scala.collection.mutable.HashMap[String, String]

      map += (nick1 + "," + nick2type -> nick2id)
      map += (nick2 + "," + nick1type -> nick1id)
      map
    })
    .reduce( (x, y)=> {
      y.foreach( a => {
        if(! x.contains(a._1)) 
          x += a
      })
      x
    }
    )
    
    
    val broadcastVar = sc.broadcast(hsmap)
    
    
    val remainvid = sc.textFile(input2).filter(_.split(",").length > 2).map(x => {
      val fields = x.split(",")
      val nick1 = fields(0).trim()
      val nick2 = fields(1).trim()
      val nick1type = nick1.split("\\|")(0).trim()
      val nick1id = nick1.split("\\|")(1).trim()
      val nick2type = nick2.split("\\|")(0).trim()
      val nick2id = nick2.split("\\|")(1).trim()
      var outputString = ""
//      System.out.println("1111111111111111111");
//      System.out.println(hsmap.contains("12223333333333"));
//      System.out.println(hsmap.contains(nick1 + "," + nick2type));
//      System.out.println(hsmap.contains(nick2 + "," + nick1type));
      if(hsmap.contains(nick1 + "," + nick2type)){
        var right = hsmap.get(nick1 + "," + nick2type).toString().trim()
//        System.out.println(right)
//        System.out.println("Some(" + nick2id + ")")
        if(right.equals("Some(" + nick2id + ")")){
          outputString = x
        }
      }
      else if(hsmap.contains(nick2 + "," + nick1type)){
        var correct = hsmap.get(nick2 + "," + nick1type).toString().trim()
        if(correct.equals("Some(" + nick1id + ")")){
          outputString = x
        }
        
      }
      else{
        outputString = x
      }
      outputString
    })
    
    
    .distinct.saveAsTextFile(output)

  }

}
