package myscala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*
 * changeinfo只要地址变更
 */

object combinechangeinfo {
  @throws[Exception]
  def main(args: Array[String]): Unit = {

    val master = "yarn-client";
    val conf = new SparkConf().setAppName("combinedata").setMaster(master)
    val sc = new SparkContext(conf)

    val inputFile1 = "/companyinfo/out/idmatched/changeinfo" //输入文件
    val outputFile1 = "/companyinfo/out/combined/changeinfo" //输出文件

    val inputFile3 = "/companyinfo/changeinfotype.txt"
    val changeinfotype = sc.textFile(inputFile3)
      .map(_.split(",", 2)).filter(_.size == 2).map(x => (x(0), x(1)))

    var rdd = sc.textFile(inputFile1).map(_.split(",", 2)).filter(_.size == 2)
      .map(x => (x(1).split("\t", -1)(0), x))
      .join(changeinfotype).map(_._2._1)
      .map(x => (x(0), x(1))).reduceByKey(_ + "\t" + _)
      .map(line => {
        val key = line._1
        val value = line._2
        key + "," + value
      })
      .saveAsTextFile(outputFile1)
  }
}