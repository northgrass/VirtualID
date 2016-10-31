import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Random
import java.net.URLEncoder
import java.net.URLDecoder
import org.apache.hadoop.hdfs.server.balancer.Balancer.Matcher
import java.util.regex.Pattern
import scala.collection.immutable
import collection.immutable.HashMap

import java.sql.Timestamp
import java.util.Calendar
import scala.util.Properties
import java.util.Properties

object OSJaccard {

  def main(args: Array[String]) {

    if (args.length != 2) {
      sys.error("Usage:<input><output>")
    }

    val Array(input, output) = args

    val sparkConf = new SparkConf().setAppName(" SID Similar based on Device")//.setMaster("local")

    sparkConf.set("spark.akka.frameSize", "100")
    val sc = new SparkContext(sparkConf)

    var userid = ""
    var itemid = ""
    // extract (userid, itemid, rating) from ratings data
    //  (jdNick|zhyh08, wow64,5.396100486399102E-8
    val oriRatings = sc.textFile(input).map(_.split(",")).filter(_.length > 2).map(fields => {

      //      val timeIndex = fields(0)
      val ua = fields(1)
      val s_id = fields(0)
      //      val coNum = fields(3).split("\\)")(0)
      //      println(coNum)
      var rating: Double = 0.0
      //      rating = 1 / (coNum.toDouble)
      //      println(rating)
      itemid = ua
      userid = s_id
      if (itemid.toString().contains("0._0._0._0")) {
        itemid = "0._0._0._0"

      }
      //            println(rating)
      (userid, itemid, 1L)

    }).filter(_._2 != "0._0._0._0")

    val ratings = oriRatings.groupBy(k => k._1).flatMap(x => (x._2.toList.sortWith((x, y) => x._3 > y._3).take(10000)))

    // one user corresponding many item
    val user2manyItem = ratings.groupBy(tup => tup._1)
    //one user corresponding number of item
    val numPrefPerUser = user2manyItem.map(grouped => (grouped._1, grouped._2.size))
    //join ratings with user's pref num
    //ratingsWithSize now contains the following fields: (user, item, rating, numPrefs).
    val ratingsWithSize = user2manyItem.join(numPrefPerUser).
      flatMap(joined => {
        joined._2._1.map(f =>

          (f._1, f._2, f._3, joined._2._2))
      })
    //(user, item, rating, numPrefs) ==>(item,(user, item, rating, numPrefs))
    val ratings2 = ratingsWithSize.keyBy(tup => tup._2)
    //ratingPairs format (t,iterator((u1,t,pref1,numpref1),(u2,t,pref2,numpref2))) and u1<u2
    //this don't double-count and exclude self-pairs
    val ratingPairs = ratings2.join(ratings2).filter(
      f =>
        f._2._1._1 < f._2._2._1)

    val tempVectorCalcs = ratingPairs.map(data => {
      val key = (data._2._1._1, data._2._2._1)
      val stats =
        (data._2._1._3 * data._2._2._3, //rating 1 * rating 2
          data._2._1._3, //rating user 1
          data._2._2._3, //rating user 2
          math.pow(data._2._1._3, 2), //square of rating user 1
          math.pow(data._2._2._3, 2), //square of rating user 2
          data._2._1._4, //num prefs of user 1
          data._2._2._4) //num prefs of user 2
      (key, stats)
    })
    val vectorCalcs = tempVectorCalcs.groupByKey().map(data => {
      val key = data._1
      val vals = data._2

      val size = vals.size
      val dotProduct = vals.map(f => f._1).sum
      val ratingSum = vals.map(f => f._2).sum
      val rating2Sum = vals.map(f => f._3).sum
      val ratingSeq = vals.map(f => f._4).sum
      val rating2Seq = vals.map(f => f._5).sum
      val numPref = vals.map(f => f._6).max
      val numPref2 = vals.map(f => f._7).max
      (key, (size, dotProduct, ratingSum, rating2Sum, ratingSeq, rating2Seq, numPref, numPref2))
    })

    //due to matrix is not symmetry(瀵圭О) , use half matrix build full matrix
    val inverseVectorCalcs = vectorCalcs.map(x => (
      //        println(x._1._2+"**"+ x._1._1)
      (x._1._2, x._1._1), (x._2._1, x._2._2, x._2._4, x._2._3, x._2._6, x._2._5, x._2._8, x._2._7)))
    //    val vectorCalcsTotal = vectorCalcs ++ inverseVectorCalcs
    val vectorCalcsTotal = vectorCalcs
    // compute similarity metrics for each movie pair,  similarities meaning user2 to user1 similarity
    val tempSimilarities =
      vectorCalcsTotal.map(fields => {
        val key = fields._1
        //        println("finalkey=" + key)
        //        println("finalValue" + fields._2)
        val (size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq, numRaters, numRaters2) = fields._2
        //        val cosSim = cosineSimilarity(dotProduct, scala.math.sqrt(ratingNormSq), scala.math.sqrt(rating2NormSq)) *
        //          size / (numRaters * math.log10(numRaters2 + 10))

        //               val cosSim = cosineSimilarity(dotProduct, scala.math.sqrt(ratingNormSq), scala.math.sqrt(rating2NormSq))

        val jaccard = jaccardSimilarity(size, numRaters, numRaters2)
        //        val corr = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
        (key._1, (key._2, jaccard, ratingSum, size, numRaters, numRaters2))
      })

    tempSimilarities.saveAsTextFile(output)

  }
  def cosineSimilarity(dotProduct: Double, ratingNorm: Double, rating2Norm: Double) = {
    dotProduct / (ratingNorm * rating2Norm)
  }
  def jaccardSimilarity(usersInCommon: Double, totalUsers1: Double, totalUsers2: Double) = {
    val union = totalUsers1 + totalUsers2 - usersInCommon
    usersInCommon / union
  }

}