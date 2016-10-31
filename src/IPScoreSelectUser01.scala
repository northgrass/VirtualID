import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer

object IPScoreSelectUser01 {

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
    val dataDeal = sc.textFile(input).filter(!_.contains("None"))
      .map(_.split(",")).filter(_.length == 5)
      .map(x => {
        var outputString = ""
        var id1_type = x(0).split("\\|")(0).replace("(", "").trim()
        var id1 = x(0).split("\\|")(1).trim()
        var id2_type = x(1).split("\\|")(0).trim()
        var id2 = x(1).split("\\|")(1).trim()
        var score1 = x(2).trim().toDouble
        var score2 = x(3).trim().toDouble
        var score3 = x(4).replace(")", "").trim().toDouble
        var score = 0.5*score1 + 0.5*score3
//        if(score1>0.3&&score2>0.2&&score3>0.4){
        	outputString = id1_type + "," + id1 + "," + id2_type + "," + id2 + "," + score1 + "," + score2 + "," + score3 + "," + score
//        }
        outputString
      }).distinct
    
    val timeAndID = dataDeal.map(_.split(",")).filter(_.length == 8)
      .map(x => {
        var id1_type = x(0)
        var id1 = x(1)
        var id2_type = x(2)
        var id2 = x(3)
        var score1 = x(4).toDouble
        var score2 = x(5).toDouble
        var score3 = x(6).toDouble
        var score4 = x(7).toDouble
//        var score5 = x(8).toDouble
//        var score6 = x(9).toDouble
//        var score7 = x(10).toDouble

        var key = id1_type + "," + id1 + "," + id2_type
        (key, (id2, score4))
      })
      .groupByKey
      .mapValues(iter => iter.toList.sortWith(_._2 > _._2)) //.take(1)
      .mapValues(iter => iter.sortBy(x => x._2)
        .reverse.take(1)).sortByKey(false)

    val timeAndID1 = dataDeal
      .map(_.split(",")).filter(_.length == 8)
      .map(x => {
        var id1_type = x(0)
        var id1 = x(1)
        var id2_type = x(2)
        var id2 = x(3)
        var score1 = x(4).toDouble
        var score2 = x(5).toDouble
        var score3 = x(6).toDouble
        var score4 = x(7).toDouble
//        var score5 = x(8).toDouble
//        var score6 = x(9).toDouble
//        var score7 = x(10).toDouble

        var key = id1_type + "," + id1 + "," + id2_type
        (key, (id2, score4))
      })
      .groupByKey
      .mapValues(iter => iter.toList.sortWith(_._2 > _._2).length)

    var outputstring = timeAndID.join(timeAndID1).saveAsTextFile(output)

  }

}