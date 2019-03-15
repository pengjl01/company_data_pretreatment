package myscala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import util.control.Breaks._
/*
 * step1
 * 统计字段
 */
object test {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    run(args)
  }
  def run(args: Array[String]): Unit = {
    val master = "yarn-client";
    val conf = new SparkConf().setAppName("countfield").setMaster(master)
    val sc = new SparkContext(conf)

    val inputFile = "/companyinfo/out/combined/businessscope" //输入文件
    val outputFile = "/companyinfo/test1" //输出文件
    val outputFile2 = "/companyinfo/test/" //输出文件
    val location = 19 //统计字段位置，0起始
    val inputFile2 = "/companyinfo/industry/industry"

    count(sc, inputFile, outputFile)
    //    val rdd = sc.textFile(inputFile).map(_.split("\t", -1)).filter(_.length == 20)
    //    .map(x =>(x(0), x(19))).filter(!_._2.equals("")).filter(!_._2.equals("-"))
    //    val useful=rdd.count()
    //    rdd.flatMapValues(_.split("；")).map(x=>(x._2,x._1))
    //    join(sc, inputFile2, 1, outputFile2, rdd)

    //    join(sc, inputFile2, 2, outputFile2, rdd)
    //    join(sc, inputFile2, 3, outputFile2, rdd)
    //    join(sc, inputFile2, 4, outputFile2, rdd)
    //    join(sc, inputFile2, 5, outputFile2, rdd)

    //    val count=rdd.count()
    //    rdd.map(_.split(",",2)).filter(_.length==2).map(x=>(x(1),1)).reduceByKey(_+_).sortBy(_._2, ascending = false)
    //      .map(line => {
    //        val word = line._1
    //        val cnt = line._2
    //        if(word.equals(""))
    //          "-,"+cnt
    //        else
    //          word + "," + cnt
    //      }).saveAsTextFile(outputFile2)
    //    println(count)
    //    println(count)
    //    println(count)
    //    println(count)
    //    println(count)
    //println("useful:"+useful)
    //println("useful:"+useful)
    //println("useful:"+useful)
    //println("useful:"+useful)
    //println("useful:"+useful)
  }
  def count(sc: SparkContext, inputFile: String , outputFile: String) = {
    val rdd = sc.textFile(inputFile).map(line => line.split(",", 2)).filter(_.length==2).map(_(1))
      .flatMap(x => x.split("\t", -1)).map((_,1)).reduceByKey(_+_)
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
  def join(sc: SparkContext, inputFile: String, index: Int, outputFile: String, rdd: org.apache.spark.rdd.RDD[(String, String)]): Unit = {
    val inputFile0 = inputFile + index + ".txt"
    val outputFile0 = outputFile + index
    //    val jingyingfanwei = sc.textFile(inputFile0).filter(!_.equals("")).map(x => (x, "")).distinct()
    //    val out = rdd.join(jingyingfanwei)
    //    val aaa=out.map(x => (x._2._1,1)).distinct()
    //    val bbb=out.map(x=>(x._1,1)).reduceByKey(_+_).saveAsTextFile("/companyinfo/test1")
    ////      .map(line => {
    ////        val word = line._1
    ////        val cnt = line._2
    ////        if (word.equals(""))
    ////          "-," + cnt
    ////        else
    ////          word + "," + cnt
    ////      })//.saveAsTextFile(outputFile0)
    //    val count=aaa.count()
    //    println("line"+count)
    //    println("line"+count)
    //    println("line"+count)
    //    println("line"+count)
    //    println("line"+count)
  }

  //  def test(sc: SparkContext, inputFile: String, location: Int, outputFile: String) {
  //    val rdd1 = sc.textFile(outputFile)
  //    val rdd = sc.textFile(outputFile).map(_.split(",", 2)).filter(_.length == 2).map(x => (x(1).toInt, x(0)))
  //      .map(x => {
  //        val id = x._1
  //        val value = x._2
  //        rdd1.foreach(find(value,_))
  //      })
  //  }
  def find(value: String, keys: Array[String]): String = {
    var ans = "-"
    breakable {
      for (i <- 0 until keys.length) {
        val key = keys(i)
        if (value.indexOf(key) != -1) {
          ans = keys(i)
          break()
        }
      }
    }
    ans
  }

}
