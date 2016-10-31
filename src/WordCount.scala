import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object SimpleApp {
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
    val timeAndID = sc.textFile(input).map(_.split(",")).filter(_.length == 9).map(fields => {
      val id1 = fields(0).split("\\(")(1)
      val id2 = fields(1).split("\\(")(1)
      val num1 = Integer.parseInt(fields(7))
      val num2 = Integer.parseInt(fields(8).split("\\)\\)")(0))
      val idtype1 = id1.split("\\|")(0)
      val idtype2 = id2.split("\\|")(0)
      var outputString = ""
      if (!(idtype1.equals(idtype2)) && (num1 > 10) && (num2 > 10)) {
        outputString = id1 + "," + id2 + "," + fields(2) + "," + fields(3) + "," + fields(4) + "," + fields(5) + "," + fields(6) + "," + num1 + "," + num2
      }
      outputString
    })
      .distinct()
      .saveAsTextFile(output)
  }
}