package myscala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object countcombined {
  def main(args: Array[String]): Unit = {

    val master = "yarn-client";
    val conf = new SparkConf().setAppName("countcombined").setMaster(master)
    val sc = new SparkContext(conf)

    val inputFile1 = args(0) //输入文件
    val outputFile1 = args(1) //输出文件
    val persize = Integer.valueOf(args(2)) //每组字段个数

    val rdd = sc.textFile(inputFile1).map(_.split(",",2)).filter(_.size==2)
    .map(x=>(x(1).split("\t",-1).size/persize,1)).reduceByKey(_+_).saveAsTextFile(outputFile1)
  }
}