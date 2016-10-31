import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object DifferentRate_Filter {
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
    val timeAndID = sc.textFile(input).filter(_.split(",").length == 4).map(fields => {
      val item = fields.split(",")
      val denominator = item(2).replace(")", "").trim()
      val numerator = item(3).replace(")", "").trim()
      val different_rate = (denominator.toDouble - numerator.toDouble)/denominator.toDouble
      var outputString = ""
      if(different_rate<0.5){
        outputString = fields.replace("(", "").replace(")", "") + "," + different_rate
      }
      outputString
    })
      .distinct()
      .saveAsTextFile(output)
  }
}