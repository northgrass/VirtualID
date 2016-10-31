import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object Mail_Correlation {
  def main(args: Array[String]) = {

    if (args.length != 2) {
      sys.error("Usage:<input><output>")
    }

    val Array(input, output) = args

    val conf = new SparkConf
    conf.setAppName("filter")
    //.setMaster("local")
    conf.set("spark.akka.frameSize", "200")
    val sc = new SparkContext(conf)
    //邮箱关联，一个邮箱对应的ID都写在一起
    /*    val timeAndID = sc.textFile(input).map(_.split(",")).filter(_.length == 4).map(fields => {
      val id = fields(0) + "|" + fields(1)
      val mail = fields(3)
      (mail,id) */
    
    //手机号关联，一个手机号对应的ID都写在一起
     val timeAndID = sc.textFile(input).map(_.split(",")).filter(_.length == 4).map(fields => {
      val id = fields(0) + "|" + fields(1)
      val phone = fields(3)
      (phone,id) 

    //手机操作系统的一个ID对应的所有手机操作系统类型
    /*val timeAndID = sc.textFile(input).filter(!_.contains("WT")).filter(!_.contains("Macosx")).filter(!_.contains("Windows")).map(_.split(",")).filter(_.length == 2).map(fields => {
      val id = fields(0)
      val os = fields(1).split("\t")(0)
      (id, os)   */

      //PC操作系统的一个ID对应的所有PC操作系统类型
      /*       val timeAndID = sc.textFile(input).map(_.split(",")).filter(_.length == 2).map(fields => {
        var id = ""
        var os = ""
        if(fields(1).contains("WT")||fields(1).contains("Macosx")||fields(1).contains("Windows")){
        id = fields(0)
        os = fields(1).split("\t")(0)
        }
        (id,os)  */

      //浏览器的一个ID对应的所有浏览器类型
      /*        val timeAndID = sc.textFile(input).map(_.split(",")).filter(_.length == 3).map(fields => {
        val id = fields(0).replace("(", "") + "|" + fields(1)
        val browser = fields(2).split("\\)")(0)
        (id,browser)  */

      //手机品牌的一个ID对应的所有手机品牌类型
      /*    val timeAndID = sc.textFile(input).filter(!_.contains("window Laptop")).filter(!_.contains("macintosh")).map(_.split(",")).filter(_.length == 2).map(fields => {
    val id = fields(0).replace("(", "")
    val brand = fields(1).split("\\)")(0)
    (id,brand)  */
    }).combineByKey((v: String) => List(v), (c: List[String], v: String) => v :: c, (c1: List[String], c2: List[String]) => c1 ::: c2)
      .distinct()
      .saveAsTextFile(output)
  }
}