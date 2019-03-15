package myscala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import util.control.Breaks._

/*
 * 生成经营范围表（值是行业编码）
 */
object getbusinessscope {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    run(args)
  }
  def run(args: Array[String]): Unit = {

    //    val master = "local[*]";
    //    val baseinfodata = "baseinfo.txt"
    //    // /companyinfo/businessscope.txt
    //    val businessscopefinddata = "businessscopefind.txt"
    //    val businessscopematchdata = "businessscopematch.txt"
    //    val output = "businessscope"
  // /companyinfo/businessscopematch.txt 精确匹配用表
  val businessscopematchdata = "/companyinfo/businessscopematch.txt"
  // /companyinfo/businessscopefind.txt 模糊匹配用表
  val businessscopefinddata = "/companyinfo/businessscopefind.txt"
  val master = "yarn-client";
  val conf = new SparkConf().setAppName("getbusinessscope").setMaster(master)
  val sc = new SparkContext(conf)
  //精确匹配用
  val businessscopematch = sc.textFile(businessscopematchdata).map(_.split("\t", 2)).filter(_.length == 2).map(x => (x(0), x(1)))
    .flatMapValues(_.split("。|，|；|：|\t|　| ", -1)).filter(_._2.length >= 2).distinct().filter(!_._2.equals("制造"))
  //    businessscopematch.foreach(println)
  //模糊匹配用
  val businessscopefind = sc.textFile(businessscopefinddata).map(_.split("\t", 2)).filter(_.length == 2)
    .map(x => (x(0), x(1))).flatMapValues(x => x.split(",", -1)).filter(!_._2.equals("")).distinct()
  val matchkeys4 = businessscopematch.filter(_._1.length == 4).collect()
  val matchkeys3 = businessscopematch.filter(_._1.length == 3).collect()
  val matchkeys2 = businessscopematch.filter(_._1.length == 2).collect()
  val findkeys4 = businessscopefind.filter(_._1.length == 4).collect()
  val findkeys3 = businessscopefind.filter(_._1.length == 3).collect()
  val findkeys2 = businessscopefind.filter(_._1.length == 2).collect()
    //从hdfs读数据
    // /companyinfo/out/sizematched/baseinfo
    val baseinfodata = "/companyinfo/out/sizematched/baseinfo"
    //输出
    val output = "/companyinfo/out/combined/businessscope"

    //    businessscopefind.foreach(println)
    val businessscope = sc.textFile(baseinfodata).map(line => line.split("\t", -1)).filter(_.size == 20)
      .map(x => (x(0), x(19)))
      .flatMapValues(x => x.split("；", -1)).filter(!_._2.equals("")).filter(!_._2.equals("-"))
      .map(x => {
        val id = x._1
        val value = x._2
        (id, myget(value,matchkeys4,matchkeys3,matchkeys2,findkeys4,findkeys3,findkeys2))
      }).filter(!_._2.equals("-")).distinct()
      .reduceByKey(_ + "\t" + _)
      .map(line => {
        val key = line._1
        val value = line._2
        key + "," + value
      })
    businessscope.saveAsTextFile(output)
    val count = businessscope.count()
    println("line" + count)
    println("line" + count)
    println("line" + count)
    println("line" + count)
    println("line" + count)

  }
  def myfind(value: String, keys: Array[(String, String)]): String = {
    var ans = "-"
    val value1 = value.replaceAll("（[^）]*）|\\([^\\)]*\\)|和|及", "")
    breakable {
      for (i <- 0 until keys.length) {
        val key = keys(i)._2
        if (value1.indexOf(key) != -1) {
          ans = keys(i)._1
          break()
        }
      }
    }
    ans
  }
  def mymatch(value: String, keys: Array[(String, String)]): String = {
    var ans = "-"
    breakable {
      for (i <- 0 until keys.length) {
        val key = keys(i)._2
        if (value.equals(key)) {
          ans = keys(i)._1
          break()
        }
      }
    }
    ans
  }
  def myget(value: String,matchkeys4: Array[(String, String)],matchkeys3: Array[(String, String)],matchkeys2: Array[(String, String)],findkeys4: Array[(String, String)],findkeys3: Array[(String, String)],findkeys2: Array[(String, String)]): String = {
    var ans = mymatch(value, matchkeys4)
    if (ans.equals("-"))
      ans = mymatch(value, matchkeys3)
    if (ans.equals("-"))
      ans = mymatch(value, matchkeys2)
    if (ans.equals("-"))
      ans = myfind(value, findkeys4)
    if (ans.equals("-"))
      ans = myfind(value, findkeys3)
    if (ans.equals("-"))
      ans = myfind(value, findkeys2)
    ans
  }
  def notfind(value: String, keys: Array[(String, String)]): String = {
    var ans = "-"
    for (i <- 0 until keys.length) {
      val key = keys(i)._2
      if (value.indexOf(key) != -1) {
        ans = keys(i)._1
      }
    }
    if (ans.equals("-"))
      ans = value
    else
      ans = "-"
    ans
  }
}