import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import collection.immutable.HashMap

object DeviceMode {
  //  deviceMap = deviceMap :+ "iphone" :+ "wow64" :+ "sony" :+ "vivo" :+ "htc" :+ "oppo" :+ "huawei" :+ "samsung" :+ "asum" :+ "infocus"
  //  deviceMap = deviceMap :+ "acer" :+ "benq" :+ "coolpad" :+ "fet" :+ "gionee" :+ "gsmart" :+ "letv" :+ "meitu" :+ "meizu" :+ "microsoft" :+ "motorola"
  //  deviceMap = deviceMap :+ "nextbit" :+ "nokia" :+ "oneplus" :+ "panasonic" :+ "twm" :+ "zte" :+ "ivvi" :+ "mi" :+ "xiaomi" :+ "redmi"
  //  deviceMap = deviceMap :+ "miui" :+ "mi-one" :+ "lenovo"

  var Devicemap = new HashMap[String, String]()
  Devicemap = Devicemap + ("iphone" -> "iphone") + ("wow64" -> "window Laptop") + ("macintosh" -> "macintosh") + ("sony" -> "sony") + ("vivo" -> "vivo") + ("htc" -> "htc")
  Devicemap = Devicemap + ("oppo" -> "oppo") + ("huawei" -> "huawei") + ("samsung" -> "samsung") + ("asum" -> "asum") + ("infocus" -> "infocus")
  Devicemap = Devicemap + ("acer" -> "acer") + ("benq" -> "benq") + ("coolpad" -> "coolpad")  + ("gionee" -> "gionee")//+ ("fet" -> "fet")
  Devicemap = Devicemap + ("gsmart" -> "gsmart") + ("letv" -> "letv") + ("meitu" -> "meitu") + ("meizu" -> "meizu") + ("microsoft" -> "microsoft")
  Devicemap = Devicemap + ("motorola" -> "motorola") + ("nextbit" -> "nextbit") + ("nokia" -> "nokia") + ("oneplus" -> "oneplus") + ("panasonic" -> "panasonic")
  Devicemap = Devicemap + ("twm" -> "twm") + ("zte" -> "zte") + ("ivvi" -> "ivvi") + ("xiaomi" -> "xiaomi") // + ("mi" -> "xiaomi")
  Devicemap = Devicemap + ("redmi" -> "xiaomi") + ("mi-one" -> "xiaomi") + ("lenovo" -> "lenovo") + ("smartisan " -> "smartisan ") //+("cfnetwork"->"iphone")

  def main(args: Array[String]) = {

    if (args.length != 2) {
      sys.error("Usage:<input><output>")
    }

    val Array(input, output) = args

    var outputString = ""

    val conf = new SparkConf
    conf.setAppName("id device output") //.setMaster("local")
    val sc = new SparkContext(conf)

    //    var serialNum = 0
    val timeAndID = sc.textFile(input)
      .map(_.split("\\|")).filter(_.length > 6)
      .map(x => {
        var id = x(0) + "|" + x(1)
        var ua = x(2)
        //        ua = "iphone"
        //        ua = "pp_ooo-p" //pp6000_143i//pp6000-143i//pp_ooo-p
        var tac_ua = x(3).toDouble
        var ua_num = x(4).toDouble
        var tac_num = x(5).toDouble
        var jacc = x(6).toDouble
        var ua_out = "a|b"
        for (device <- Devicemap.keySet) {
          if (ua.equals(device)) {//if (ua.contains(device))
            ua_out = Devicemap(device)
          }

        }

        //     (id, (ua_out, tac_ua, ua_num, tac_num, jacc))
        (id, (ua_out))

      }).filter(_._2 != "a|b").distinct
      .saveAsTextFile(output)
  }

}