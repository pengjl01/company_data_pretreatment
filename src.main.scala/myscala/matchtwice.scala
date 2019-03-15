package myscala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import util.control.Breaks._
/*
 * 找到切割后匹配会有冲突的项，加到第三列
 * 本地运行不必要提交
 */
object matchtwice {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    run(args)
  }
  def run(args: Array[String]): Unit = {
    val master = "local[*]";
    val conf = new SparkConf().setAppName("dealbusinessscope").setMaster(master)
    val sc = new SparkContext(conf)

    // id\t关键字(切分前)
    //即行业excel拷贝出来
    val businessscopetypedata = "123.txt"
    val output = "businessscope1"

    val businessscopetype = sc.textFile(businessscopetypedata)
      .map(_.split("\t", 2)).filter(_.length == 2).map(x => (x(0), x(1)))

    val temp = businessscopetype.map(_._2).collect()

    businessscopetype.map(line => {
      val key = line._1
      val value = line._2.split("，|、", -1)
      val vvv = value.map(find(_, temp))
      var bbb = ""
      for (i <- 0 until vvv.length) {
        val key = vvv(i)
        if (!key.equals("")) {
          bbb+=key+"，"
        }
      }
      key+","+line._2+","+bbb
    }).saveAsTextFile(output)
  }
  def find(value: String, keys: Array[String]): String = {
    var a = 0
    for (i <- 0 until keys.length) {
      val key = keys(i)
      if (key.indexOf(value) != -1) {
        a += 1
      }
    }
    if (a >= 2)
      value
    else
      ""
  }
}