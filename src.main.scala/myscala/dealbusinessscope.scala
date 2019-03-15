package myscala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import util.control.Breaks._
/*
 * 经营范围简单粗暴分割
 */
object dealbusinessscope {
    @throws[Exception]
  def main(args: Array[String]): Unit = {
    run(args)
  }
  def run(args: Array[String]): Unit = {
    val master = "local[*]";
    val conf = new SparkConf().setAppName("dealbusinessscope").setMaster(master)
    val sc = new SparkContext(conf)
    
    val businessscopetypedata = "/companyinfo/businessscope.txt"
    val output = "/companyinfo/businessscope1"
    
    val businessscopetype = sc.textFile(businessscopetypedata)
    .map(_.split("\t",2)).filter(_.length==2).map(x=>(x(0),x(1)))
    .flatMapValues(_.split("。|，|；|：|\t",-1))
    .filter(_._2.length>=2).distinct()
    .map(line => {
        val key = line._1
        val value = line._2
        key + "," + value
      }).saveAsTextFile(output)
  }
}