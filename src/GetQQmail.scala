import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object GetQQmail {
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
    val timeAndID = sc.textFile(input).map(_.split(",")).filter(_.length == 4).map(fields => {
      val mail = fields(3)
      var outputString = ""
      if(mail.contains("@qq")){
        if(mail.split("@qq")(0).matches("^[0-9]*$")){
        	outputString = mail + "," + fields(0) + "|" + fields(1) + "," + "QQ_normal|" + mail.split("@qq")(0)
        }
      }
      outputString
    })
      .distinct()
      .saveAsTextFile(output)
  }
}