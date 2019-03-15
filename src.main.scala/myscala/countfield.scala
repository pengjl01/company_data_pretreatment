package myscala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/*
 * step1
 * 统计字段 
 */
object countfield {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val master = "yarn-client";
    val conf = new SparkConf().setAppName("countfield").setMaster(master)
    val sc = new SparkContext(conf)

    val inputFile = args(0) //输入文件
    val outputFile = args(1) //输出文件
    val location=Integer.valueOf(args(2))//统计字段位置，0起始

    val rdd = sc.textFile(inputFile).map(line => line.split("\t",-1))
    .filter(_.length>location).map(x => (x(location), 1)).reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)      
      .map(line => {
        val word = line._1
        val cnt = line._2
        if(word.equals(""))
          "-,"+cnt
        else
          word + "," + cnt
      }).saveAsTextFile(outputFile)
//    println(rdd.count())
    //println(accum.value)
  }
}
