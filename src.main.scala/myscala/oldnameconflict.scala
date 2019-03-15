package myscala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/*
 * step0.1
 * 提取有曾用名的公司
 *
 */
object oldnameconflict {

  @throws[Exception]
  def main(args: Array[String]): Unit = {

    val master = "yarn-client";
    //val master="local[*]"
    val conf = new SparkConf().setAppName("oldnameconflict").setMaster(master)
    val sc = new SparkContext(conf)

    val inputFile2 = "/companyinfo/out/sizematched/changeinfo" //输入目录
    val outputFile1 = "/companyinfo/out/oldname" //输出目录
    val outputFile = "/companyinfo/out/oldnameconflict" //输出目录
    val inputFile3 = "/companyinfo/changetype.txt"
    val namechangetype = sc.textFile(inputFile3)
      .map(_.split(",",-1)).filter(_.size < 2).map(x => (x(0), x(1)))
    //val test=sc.textFile(inputFile3).saveAsTextFile("/companyinfo/out/temp1")
    val namechange = sc.textFile(inputFile2)
      .map(_.split("\t",-1)).filter(_(0).length == 32)
      .map(x => (x(1), x(3) + "\t" + x(4) + "\t" + x(0)))
      .join(namechangetype).map(_._2._1.split("\t",-1))
    //namechange.saveAsTextFile("/companyinfo/out/temp")
    val oldname = namechange.map(x => (x(0), x(2)))
    val oldnameconflict = namechange.map(x => (x(1), x(2))).join(oldname).filter(x => (!x._2._1.equals(x._2._2))).distinct().saveAsTextFile(outputFile)
    oldname.map(x => {
      val name = x._1
      val id = x._2
      id + "\t" + name
    }).distinct().saveAsTextFile(outputFile1)
  }
}