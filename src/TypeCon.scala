import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object TypeCon {
  def main(args: Array[String]) = {

    if (args.length != 2) {
      sys.error("Usage:<input><output>")
    }

    val Array(input, output) = args

    //    var outputString = ""

    val conf = new SparkConf
    conf.setAppName("filter")
    .setMaster("local")
    conf.set("spark.akka.frameSize", "200")
    val sc = new SparkContext(conf)
    val timeAndID = sc.textFile(input).repartition(30).map(_.split("\\|")).filter(_.length == 8).map(fields => {
      val x=fields(3)
      x
    }).distinct.map(x => (x,1)).reduceByKey(_ + _)
      .saveAsTextFile(output)
  }
}