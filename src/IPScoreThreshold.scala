import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object IPScoreThreshold {
  def main(args: Array[String]) = {

    if (args.length != 2) {
      sys.error("Usage:<input><output>")
    }

    val Array(input, output) = args

    //    var outputString = ""

    val conf = new SparkConf
    conf.setAppName("filter")
    //.setMaster("local")
    conf.set("spark.akka.frameSize", "200")
    val sc = new SparkContext(conf)
    val timeAndID = sc.textFile(input).map(_.split(",")).filter(_.length == 6).map(fields => {
      val id1type = fields(0).replace("(", "").trim()
      val id1 = fields(1).trim()
      val id2type = fields(2).trim()
      val id2 = fields(3).replace("(List((", "").trim()
      val num1 = fields(4).replace(")", "").trim().toDouble
      var outputString = ""
      if (num1 >0.3) {
        outputString = id1type + "|" + id1 + "," + id2type + "|" + id2 + "," + num1
      }
      outputString
    })
      .distinct()
      .saveAsTextFile(output)
  }
}