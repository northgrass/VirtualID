import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object Mail_Correlation_DealOutput {
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
    val timeAndID = sc.textFile(input).map(_.split("List\\(")).filter(_.length == 2).map(fields => {
      val matchedids = fields(1).split("\\)\\)")(0)
      val mail = fields(0).split("\\(")(1)
      var outputString = ""
        //邮箱关联使用
     /* if(matchedids.contains("JD_Nick")&&matchedids.contains("sinaWeibo_ID")){
        */
        
        //手机号关联使用
        if(matchedids.contains("JD_Nick")&&matchedids.contains("taobao_Nick")){
        outputString = mail + matchedids
      }
      outputString
    }).distinct()
      .saveAsTextFile(output)
  }
}