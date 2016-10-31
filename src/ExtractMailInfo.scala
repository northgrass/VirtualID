import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object ExtractMailInfo {
  def main(args: Array[String]) = {

    if (args.length != 2) {
      sys.error("Usage:<input><output>")
    }

    val Array(input, output) = args

    //    var outputString = ""

    val conf = new SparkConf
    conf.setAppName("extract-mail-info")
    //.setMaster("local")
    conf.set("spark.akka.frameSize", "200")
    val sc = new SparkContext(conf)
    val timeAndID = sc.textFile(input).map(_.split("\\|")).filter(_.length == 8).map(fields => {
      val judge = fields(1)

      if (judge != "") {
        val idtype1 = fields(1)
        val idtype2 = fields(3)
        val name1 = fields(2)
        val name2 = fields(4)
        var outputString = ""
        if ((idtype1.contains("sinaWeibo_Mail") && idtype2.contains("JD_Email")) || (idtype2.contains("sinaWeibo_Mail") && idtype1.contains("JD_Email"))) {
          if(name1.matches("\\w+([-+.]\\w+)*(@|%2540|%40)\\w+([-.]\\w+)*\\.\\w+([-.]\\w+)*")&&name2.matches("\\w+([-+.]\\w+)*(@|%2540|%40)\\w+([-.]\\w+)*\\.\\w+([-.]\\w+)*")){
            outputString = idtype1+","+name1.replace("%2540", "@").replace("%40", "@")+","+idtype2+","+name2.replace("%2540", "@").replace("%40", "@")
          }
        }
        else if(idtype1.contains("sinaWeibo_Mail")||idtype1.contains("JD_Email")){
            	if(name1.matches("\\w+([-+.]\\w+)*(@|%2540|%40)\\w+([-.]\\w+)*\\.\\w+([-.]\\w+)*"))
            	{
            	  outputString = idtype1+","+name1.replace("%2540", "@").replace("%40", "@")+","+idtype2+","+name2
            	}
            }
        else if(idtype2.contains("sinaWeibo_Mail")||idtype2.contains("JD_Email")){
            	if(name2.matches("\\w+([-+.]\\w+)*(@|%2540|%40)\\w+([-.]\\w+)*\\.\\w+([-.]\\w+)*"))
            	{
            	  outputString = idtype1+","+name1+","+idtype2+","+name2.replace("%2540", "@").replace("%40", "@")
            	}
            }
        outputString 
          

      }
    })
      .distinct()
      .saveAsTextFile(output)
  }
}