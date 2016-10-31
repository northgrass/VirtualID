import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object Text11 {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Filter").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val data = sc.parallelize(List((0, 2), (0, 4), (1, 0), (1, 10), (1, 20)))
    data.map(r => (r._1, (r._2, 1))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).map(r => (r._1, (r._2._1 / r._2._2))).foreach(x => println(x))
  }
}
