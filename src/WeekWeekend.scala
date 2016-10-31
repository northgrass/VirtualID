import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object WeekWeekend {
  def main(args: Array[String]) = {

    if (args.length != 3) {
      sys.error("Usage:<input><output1><output2>")
    }

    val Array(input, output1,output2) = args

    //    var outputString = ""

    val conf = new SparkConf
    conf.setAppName("filter")
    //.setMaster("local")
    conf.set("spark.akka.frameSize", "200")
    val sc = new SparkContext(conf)
    val Weekday = sc.textFile(input).map(_.split(",")).filter(_.length == 5).map(fields => {
      val tag = fields(0)
      val time = fields(1)
      val ip = fields(2).trim()
      val id = fields(3).trim()
      var outputString = ""
      if (tag.contains("0")) {
        outputString = time + "," + ip + "," + id + "," + fields(4)
      }
      outputString
    })
      .distinct()
      .saveAsTextFile(output1)
      
      val Weekend = sc.textFile(input).map(_.split(",")).filter(_.length == 5).map(fields => {
      val tag = fields(0)
      val time = fields(1)
      val ip = fields(2).trim()
      val id = fields(3).trim()
      var outputString = ""
      if (tag.contains("1")) {
        outputString = time + "," + ip + "," + id + "," + fields(4)
      }
      outputString
    })
      .distinct()
      .saveAsTextFile(output2)
      
  }
}