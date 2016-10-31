import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object 	UnionIP_UserNum {
  


  def main(args: Array[String]) = {

    if (args.length != 2) {
      sys.error("Usage:<input><output>")
    }

    val Array(input, output) = args

    var outputString = ""

    val conf = new SparkConf
    conf.setAppName("1h IP-ID")//.setMaster("local")
//    conf.set("spark.akka.frameSize", "100")
    //     conf.set("spark.akka.frameSize", "100")  
    conf.set("spark.akka.frameSize", "500")

    conf.set("spark.akka.askTimeout", "600")
    conf.set("spark.akka.timeout", "600")
    
    // akka.pattern.AskTimeoutException: Timed out

    conf.set("spark.shuffle.file.buffer.kb", "500")
    conf.set("spark.kryoserializer.buffer.mb", "512")

    conf.set("spark.core.connection.auth.wait.timeout", "300")
    conf.set("spark.shuffle.consolidateFiles ", "true")
    val sc = new SparkContext(conf)

    var weight = 0.0

    val timeAndID = sc.textFile(input).map(_.split(",")).filter(_.length > 3).map(x => {
      var timeStr_ip = x(0)+","+x(1)
//      var cnt =x(1)
      var key = timeStr_ip
      (key,1L)
    })
    
    val all = timeAndID.reduceByKey((x, y) => x + y)
//    
    val original = sc.textFile(input).map(_.split(",")).filter(_.length > 3).map(x => {
      var timeStr_ip = x(0)+","+x(1)
//      var cnt =x(1)
      var key = timeStr_ip
      var value = x(2)
      (key,value)
    })
    
    val merge = original.join(all).map{
       case (id, (username, count)) => (id, username, count) 
    }
    
    val out = merge.saveAsTextFile(output)
    
    
    

//    all.saveAsTextFile(output)
      //      println("aaaa")


}
}