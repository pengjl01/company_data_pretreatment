package myscala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*
 * 提取搞事情瞎改名字公司
 * 假设A公司a->b;b->c
 * B公司d->a
 * 会有如下记录：
 * A a b
 * A b c
 * B d a
 * 
 * 对于提取得到的A a;B a，A!=B，表示有搞事企业存在，此名字全舍弃
 * 对于提取得到的A b;A b，A==A，表示同一企业多次改名，正常情况，保留
 */

object dealcompanyconflict {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val master = "yarn-client";
    val conf = new SparkConf().setAppName("dealcompanyconflict").setMaster(master)
    val sc = new SparkContext(conf)

    val inputFile = "/companyinfo/out/oldnameconflict" //输入文件
    val outputFile = "/companyinfo/out/oldnamedealed" //输出文件
    val inputFile2 = "/companyinfo/out/oldname"
    def getsome(x: Option[String]) = x match {
      case Some(a) => a
      case None    => ""
    }

    val oldnameconflictname = sc.textFile(inputFile).map(line => line.split("\\(|,", 3)).filter(_.size == 3).map(x => (x(1), 1)).reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .map(x => (x._1, "`")) //name `
    val oldname = sc.textFile(inputFile2).map(line => line.split("\t",-1)).filter(_.size == 2).map(x => (x(1), x(0))) // name id
      //(name,(id,some(-)))
      .leftOuterJoin(oldnameconflictname)
      .map(x => {
        val name = x._1
        val key1 = getsome(x._2._2)
        val id = x._2._1
        if (key1.equals("") && id.length == 32) //无匹配,此行需要
          id + "," + name
      }).distinct()
    oldname.saveAsTextFile(outputFile)
  }
}