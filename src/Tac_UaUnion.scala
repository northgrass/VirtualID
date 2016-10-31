import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*将tac_ua.tac.ua值结合，并计算jaccard值*/
/*
 * 输入：input_Tac和input_UA两个文件，
 * 输出：tac + "|" + ua + "|" + tac_ua + "|" + ua_num + "|" + tac_num + "|" + jaccard
 */

object Tac_UaUnion {
  def main(args: Array[String]) = {

    if (args.length != 3) {
      sys.error("Usage:<input1><input2><output>")
    }

    val Array(input_Tac, input_UA, output) = args

    var outputString = ""

    val conf = new SparkConf
    conf.setAppName("UnionTac_UA ")//.setMaster("local")
    conf.set("spark.akka.frameSize", "100")
    //     conf.set("spark.akka.frameSize", "100")  
    val sc = new SparkContext(conf)

    var weight = 0.0

    val tac = sc.textFile(input_Tac).map(_.split("\\|")).filter(_.length > 3).map(x => {
      var tac = x(0) + "|" + x(1)
      var ua = x(2)
      var tac_ua = x(3).toInt
      var tac_num = x(4)
      //      println(ua)
      //      println(tac_num)
      (tac, tac_num) //safari, QQ_normal|1059767949|3|48

    })

    //    val all = timeAndID.reduceByKey((x, y) => x + y)
    //    
    val ua = sc.textFile(input_UA).map(_.split("\\|")).filter(_.length > 3).map(x => {
      var ua = x(0)
      //      println("1=" + ua)
      var tac = x(1) + "|" + x(2)
      var tac_ua = x(3).toInt
      var ua_num = x(4)
      var value = ua + "|" + tac_ua + "|" + ua_num

      (tac, value) //safari     JD_MSISDN|13876581230|1|1502
    })

    val merge = ua.join(tac).map {
      case (tac, (value, tac_num)) => (tac, value, tac_num)
    }.map(x => s"${x._1}|${x._2}|${x._3}").distinct()
//    .map(_.split("\\|")).map(x => {
//      var tac = x(0) + "|" + x(1)
//      var ua = x(2)
//    var tac_ua = x(3).toDouble
//      var ua_num = x(4).toDouble
//      var tac_num = x(5).toDouble
//      var jaccard: Double = (tac_ua) / (ua_num + tac_num - tac_ua)
//      (tac, (ua, tac_ua, ua_num, tac_num))
//    }).groupByKey.mapValues(iter => iter.toList.sortWith(_._3 > _._3).take(3)).sortByKey(false).saveAsTextFile(output)

    val jacc = merge.map(_.split("\\|")).filter(_.length > 5).map(x => {

      //      println("1=" + ua)
      var tac = x(0) + "|" + x(1)
      var ua = x(2)
      var tac_ua = x(3).toDouble
      var ua_num = x(4).toDouble
      var tac_num = x(5).toDouble
      var jaccard: Double = (tac_ua) / (ua_num + tac_num - tac_ua)
      (tac + "|" + ua + "|" + tac_ua + "|" + ua_num + "|" + tac_num + "|" + jaccard)

    }).saveAsTextFile(output) //

    //    all.saveAsTextFile(output)
    //      println("aaaa")

  }
}