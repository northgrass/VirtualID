import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer

object DeleteQQmusic {

  def main(args: Array[String]) = {

    if (args.length != 2) {
      sys.error("Usage:<input><output>")
    }

    val Array(input, output) = args

    var outputString = ""

    val conf = new SparkConf
    conf.setAppName("id device output")//.setMaster("local")
    val sc = new SparkContext(conf)

    //    var serialNum = 0
    val dataDeal = sc.textFile(input)
      .map(_.split(",")).filter(_.length == 6)
      .map(x => {
        var outputString = ""
        var id1_type = x(0).replace("(", "").replace("QQMusic", "QQ_normal").trim()
        var id1 = x(1).trim()
        var id2_type = x(2).replace("QQMusic", "QQ_normal").trim()
        var id2 = x(3).replace("(List((", "").trim()
        var score1 = x(4).replace(")", "").trim()
        var score2 = x(5).replace(")", "").trim()
        if(!(id1_type.contains("QQ_normal")&&id2_type.contains("QQ_normal"))){
          
        	outputString = id1_type + "," + id1 + "," + id2_type + "," + id2 + "," + score1 + "," + score2 
        }
        outputString
      }).distinct.saveAsTextFile(output)


  }

}