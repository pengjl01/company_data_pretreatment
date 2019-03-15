package myscala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.{ Try, Success, Failure }
import util.control.Breaks._
object localtest {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    run(args)
  }
  def run(args: Array[String]): Unit = {
    val master = "local[*]";
    val conf = new SparkConf().setAppName("countfield").setMaster(master)
    val sc = new SparkContext(conf)

    val inputFile = "companytype.txt" //输入文件
    val inputFile2 = "newtableall.txt" //输入文件
    val outputFile = "test1" //输出文件

    val companytype = sc.textFile(inputFile).map(x => {
      val values = x.split(",", -1)
      (values(0), values(1))
    })
    val companytypeold = sc.textFile(inputFile2).map((_, "-"))

    val rdd = companytype.leftOuterJoin(companytypeold)
      .map(x => {
        val value = getsome(x._2._2)
        if (value.equals(""))
          (x._1, x._2._1)
        else
          ("-", "-")
      }).filter(!_._1.equals("-"))
      .sortBy(_._2, ascending = false)
      .map(line => {
        val word = line._1
        val cnt = line._2
        if (word.equals(""))
          "-," + cnt
        else
          word + "," + cnt
      }).saveAsTextFile(outputFile)
  }
  def getsome(x: Option[String]) = x match {
    case Some(a) => a
    case None    => ""
  }
}