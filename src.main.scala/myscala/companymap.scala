package myscala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


/*
 * 生成ID name 表，需要整合oldname
 */
object companymap {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val master = "yarn-client";
    //    val master="local[*]"
    val conf = new SparkConf().setAppName("companymap").setMaster(master)
    val sc = new SparkContext(conf)

    val inputFile1 = args(0) //baseinfo
    val inputFile2 = args(1) //oldnamedealed
    val outputFile = args(2) //输出文件

    val oldnamedealed = sc.textFile(inputFile2)
      .map(line => line.split(",",-1))
      .filter(_.size == 2).map(x => (x(1), x(0))) // name id
    val basename = sc.textFile(inputFile1)
      .map(line => line.split("\t",-1))
      .filter(_.size == 20).map(x => (x(1), x(0))) // name id
      .union(oldnamedealed).distinct()
      //去除名字相同但ID不同的（通常为杂质，少部分为改名错乱，也应予以去除）
      .reduceByKey(_+","+_).filter(_._2.split(",",-1).size==1)
      .map(line => {
        val id = line._2
        val name = line._1
        id + "," + name
      })
    basename.saveAsTextFile(outputFile)
    println(basename.count())
  }
}