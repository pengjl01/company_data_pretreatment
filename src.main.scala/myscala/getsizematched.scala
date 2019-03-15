package myscala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat
import java.text.ParseException

/*
 * step0
 * 根据切分长度过滤数据
 */

object getsizematched {
  @throws[Exception]
  def main(args: Array[String]): Unit = {

    val master = "yarn-client";
    val conf = new SparkConf().setAppName("getsizematched").setMaster(master)
    val sc = new SparkContext(conf)

    val inputFile1 = args(0) //输入目录
    val outputFile = args(1) //输出目录
    val datalength = Integer.valueOf(args(2)) //应有字段数

    var rdd = sc.textFile(inputFile1)
    if (datalength == 5) //changeinfo
    {
      rdd = rdd.map(_.split("\t", -1)).filter(_.size == datalength)
        .map(line => {
          val tf = new SimpleDateFormat("yyyy-mm-dd")
          val id = line(0)
          val data1 = line(1)
          val data2 = line(2)
          val data3 = line(3)
          val data4 = line(4)
          try {
            tf.parse(data2)
            id + "\t" + data1 + "\t" + data2 + "\t" + data3 + "\t" + data4
          } catch {
            case e: ParseException =>
              id + "\t" + data1 + "\t" + data4 + "\t" + data2 + "\t" + data3
          }
        })
    } else {
      rdd = rdd.filter(_.split("\t", -1).size == datalength)
    }

    rdd.saveAsTextFile(outputFile)
  }
  def changeindex(data: Array[String]): Array[String] = {
    val tf = new SimpleDateFormat("yyyy-mm-dd")
    var c = Array[String]()
    try {
      tf.parse(data(0))
      c = Array(data(0), data(2), data(1))
      return c
    } catch {
      case e: ParseException =>
        c = Array(data(1), data(0), data(2))
        return c
    }
  }
}