import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object ExtractPhoneInfo {
  def main(args: Array[String]) = {

    if (args.length != 2) {
      sys.error("Usage:<input><output>")
    }

    val Array(input, output) = args

    //    var outputString = ""

    val conf = new SparkConf
    conf.setAppName("extract-phone-info")
    //.setMaster("local")
    conf.set("spark.akka.frameSize", "200")
    val sc = new SparkContext(conf)
    val timeAndID = sc.textFile(input).repartition(30).map(_.split("\\|")).filter(_.length == 8).map(fields => {
      val judge = fields(1)

      if (judge != "") {
        val idtype1 = fields(1)
        val idtype2 = fields(3)
        val name1 = fields(2)
        val name2 = fields(4)
        var outputString = ""
//        if (idtype1.contains("sinaWeibo_Mail") ||idtype2.contains("JD_Email") || idtype2.contains("sinaWeibo_Mail") || idtype1.contains("JD_Email")) {
//          if(name1.matches("^1(3[0-9]|4[57]|5[0-35-9]|7[01678]|8[0-9])\\d{8}$")&&name2.matches("^1(3[0-9]|4[57]|5[0-35-9]|7[01678]|8[0-9])\\d{8}$")){
//            outputString = idtype1+","+name1 +","+idtype2+","+name2
//          }
//        }
        
        
         if ((idtype1.contains("JD_MSISDN") && idtype2.contains("taobao_MSISDN")) || (idtype2.contains("JD_MSISDN") && idtype1.contains("taobao_MSISDN"))) {
          if(name1.matches("^1(3[0-9]|4[57]|5[0-35-9]|7[01678]|8[0-9])\\d{8}$")&&name2.matches("^1(3[0-9]|4[57]|5[0-35-9]|7[01678]|8[0-9])\\d{8}$")){
  
            outputString = idtype1+","+name1 +","+idtype2+","+name2
          }
        }
        else if(idtype1.contains("JD_MSISDN")||idtype1.contains("taobao_MSISDN")){
            	if(name1.matches("^1(3[0-9]|4[57]|5[0-35-9]|7[01678]|8[0-9])\\d{8}$"))
            	{
            	  outputString = idtype1+","+name1 +","+idtype2+","+name2
            	}
            }
        else if(idtype2.contains("JD_MSISDN")||idtype2.contains("taobao_MSISDN")){
            	if(name2.matches("^1(3[0-9]|4[57]|5[0-35-9]|7[01678]|8[0-9])\\d{8}$"))
            	{
            	  outputString = idtype1+","+name1 +","+idtype2+","+name2
            	}
            }
        outputString 
          

      }
    })
      .distinct()
      .saveAsTextFile(output)
  }
}