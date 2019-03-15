package myscala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.matching.Regex

/*
 * 多行型合并为一行
 */

object combinepartner {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    //    val a="詹景春	自然人股东	79.600%	2024-12-03	4519.688"
    //    val b="林金根	自然人股东	0.732600		1.1605%"
    //    println (changeindex(a))
    //    println (changeindex(b))
    //
//        val a = "2d1.4"
//        val b = findnum1(a)
//        println(b)
    run(args)
  }
  def changeindex(data: String): String = {
    var sp = data.split("\t", -1)
    if (sp(2).split("%", -1).size > 1)
      return sp(0) + "\t" + sp(1) + "\t" + sp(4) + "\t" + sp(3) + "\t" + sp(2)
    else
      return sp(0) + "\t" + sp(1) + "\t" + sp(2) + "\t" + sp(3) + "\t" + sp(4)
  }
  def getsome(x: Option[String]) = x match {
    case Some(a) => a
    case None    => "0"
  }
  def run(args: Array[String]): Unit = {
    val master = "yarn-client";
    val inputFile1 = "/companyinfo/out/idmatched/partner" //输入文件
    val outputFile1 = "/companyinfo/out/combined/partner" //输出文件

    //    val master = "local[*]";
    //    val inputFile1 = "partner.txt" //输入文件
    //    val outputFile1 = "partner" //输出文件
    val conf = new SparkConf().setAppName("combinedata").setMaster(master)
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(inputFile1).map(_.split(",", 2))
      .filter(_.size == 2).map(x => (x(0), changeindex(x(1))))
      .map(x => (x._1, (x._2, findnum(x._2.split("\t", -1)(4)))))
      .groupByKey()
      .mapValues(x => x.toArray.sortBy(_._2)).map(x => {
        val key = x._1
        val data2 = x._2
        var ans = data2(data2.size - 1)._1
        for (i <- 1 until data2.size)
          ans = ans + "\t" + data2(data2.size - i - 1)._1
        key + "," + ans
      })
    rdd.saveAsTextFile(outputFile1)
    //      .map(line => {
    //        val key = line._1
    //        val data1 = line._2.toArray //.map(x=>)//.sortBy(_.split("/t", -1)(4))
    //        val data2=sc.makeRDD(data1).map(x=>(x.split("/t", -1)(4),x)).sortBy(_._1).map(_._2).collect()
    //        var ans = data2(0)
    //        for (i <- 1 until data2.size)
    //          ans = ans + "\t" + data2(i)
    //        key+","+ans
    //      })
    //    rdd.foreach(println)
    //    //      .reduceByKey(_ + "\t" + _)
    //    //      .map(line => {
    //    //        val key = line._1
    //    //        val value = line._2
    //    //        key + "," + value
    //    //      })
    //    rdd.saveAsTextFile(outputFile1)
  }
  def findnum(str: String): Double = {
    val pattern = "\\d+\\.\\d*".r
    val pattern1 = "\\d+".r
    var tmp = getsome(pattern findFirstIn str)
    if (tmp.equals(""))
      tmp = getsome(pattern1 findFirstIn str)
    tmp.toDouble
  }
//    def findnum1(str: String): Double = {
//    val pattern = "\\d+\\.?\\d*".r
//    var tmp = getsome(pattern findFirstIn str)
//    tmp.toDouble
//  }
}