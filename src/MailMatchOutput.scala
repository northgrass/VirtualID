import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object MailMatchOutput {
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
    val outDirect = sc.textFile(input).filter(_.split(",").length == 3).distinct()
    val outNeedDeal = sc.textFile(input).filter(_.split(",").length > 3).map(fields => {
    //邮箱关联使用
    /*  var x = fields.split("sinaWeibo_ID")  */
    //手机号关联使用
      var x = fields.split("taobao_Nick")
      val leng = x.length
      var JD_id = ""
      var sina_id = ""
      var mail = ""
      if (leng == 2) {
        var item = fields.split(",")
        val len = item.length
        mail = item(0)
        var j = 0
        for (i <- 0 until len) {
          if (item(i).contains("JD_Nick")) {
            var k = item(i).length()
            if (k > j) {
              j = k
              JD_id = item(i)
            }
          } else {
            sina_id = item(i)
          }
        }
      }
      mail + "," + JD_id+","+sina_id
    }).distinct
    var outputString = outDirect.union(outNeedDeal).distinct
    val out1 = outputString.saveAsTextFile(output)

  }
}