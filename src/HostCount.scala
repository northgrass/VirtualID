import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object HostCount {
  def main(args: Array[String]) {
    if (args.length != 2 ){
      println("usage is org.test.WordCount <input> <output>")
      return
    }
    val conf = new SparkConf().setAppName("Count Spark")
   
    //val conf = new SparkConf().setAppName("Host Count Spark").setMaster("local")
    //val conf = new SparkConf().setAppName("Top K").setMaster("local")
    conf.set("spark.akka.frameSize", "200")
    val sc=new SparkContext(conf)
    val textFile = sc.textFile(args(0))
    val result=textFile.filter(_.split(",").length>7).map(x => {
      val xx=x.split(",");
      val x2=xx(4);
      val uid=xx(6)
      (x2,uid)
    }).distinct.map(x => (x._2,1)).reduceByKey(_ + _)
   
    //result.saveAsTextFile(args(1))
//    val sorted=result.map(x => (x._2, x._1)).sortByKey(false, 1)
//    val sorted1=sorted.map(x => {
//      x._2+"\t"+x._1
//    })
    result.saveAsTextFile(args(1))
//    val topk=sorted.top(args(2).toInt)
//    val topk1=sc.parallelize(topk).map(x => x._2+"\t"+x._1)
//    topk1.saveAsTextFile(args(1))
  }
}