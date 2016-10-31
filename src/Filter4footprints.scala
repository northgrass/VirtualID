import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object Filter4footprints {
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
    val timeAndID = sc.textFile(input).filter(_.split(",").length == 21).map(fields => {
      val item = fields.split(",")
      val mobileOSjacc = item(11).trim()
      val PCOSjacc = item(14).trim()
      val brandjacc = item(17).trim()
      val browserjacc = item(20).trim()
      var outputString = ""
      if(!mobileOSjacc.equals("0.0") && !PCOSjacc.equals("0.0") && !brandjacc.equals("0.0") && !browserjacc.equals("0.0")){
        outputString = fields
      }
      outputString
    })
      .distinct()
      .saveAsTextFile(output)
  }
}