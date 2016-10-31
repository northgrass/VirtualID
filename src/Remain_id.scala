import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object Remain_id {
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
    val ID1 = sc.textFile(input).map(_.split(",")).filter(_.length > 2).map(fields => {
      val id1 = fields(0).replace("(", "").trim()
      val id2 = fields(1).trim()
      id1
    }).distinct
    
      val ID2 = sc.textFile(input).map(_.split(",")).filter(_.length > 2).map(fields => {
      val id1 = fields(0).replace("(", "").trim()
      val id2 = fields(1).trim()
      id2
    }).distinct
    
    val ID = ID1.union(ID2)
      .distinct()
      .saveAsTextFile(output)
  }
}