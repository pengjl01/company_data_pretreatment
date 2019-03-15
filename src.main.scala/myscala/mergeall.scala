package myscala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/*
 * step6
 * 全部合一起，完事儿了
 */
object mergeall {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val master = "yarn-client";
    val conf = new SparkConf().setAppName("mergeall").setMaster(master)
    val sc = new SparkContext(conf)

    val baseinfo = "/companyinfo/out/sizematched/baseinfo"
    //val branches = "/companyinfo/out/idmatched/branches"
    val changeinfo = "/companyinfo/out/combined/changeinfo"
    val partner = "/companyinfo/out/combined/partner"
    val investee = "/companyinfo/out/combined/investee" //idA,投资数额 投资占比 公司名 法人 注册资金 成立日期 经营状态 核准日期
    val businessscope = "/companyinfo/out/combined/businessscope"
    val output = "/companyinfo/allmerged" //输出文件
    //val output1 = "/companyinfo/sorteddata"

    def getsome(x: Option[String]) = x match {
      case Some(a) => a
      case None    => ""
    }

    //val branchesrdd = sc.textFile(branches).map(_.split(",", 2)).filter(_.size == 2).map(x => (x(0), x(1)))
    val changeinfordd = sc.textFile(changeinfo).map(_.split(",", 2)).filter(_.size == 2).map(x => (x(0), x(1)))
    val partnerrdd = sc.textFile(partner).map(_.split(",", 2)).filter(_.size == 2).map(x => (x(0), x(1)))
    val investeerdd = sc.textFile(investee).map(_.split(",", 2)).filter(_.size == 2).map(x => (x(0), x(1)))
    val businessscoperdd = sc.textFile(businessscope).map(_.split(",", 2)).filter(_.size == 2).map(x => (x(0), x(1)))

    val rdd = sc.textFile(baseinfo).map(_.split("\t", 2))
      .filter(_.size == 2).map(x => (x(0), x(1)))
      //      .leftOuterJoin(branchesrdd)
      //      .map(line => {
      //        val id = line._1
      //        val basedata = line._2._1
      //        val adddata = getsome(line._2._2)
      //        (id, basedata + "," + adddata)
      //      })
      .leftOuterJoin(partnerrdd)
      .map(line => {
        val id = line._1
        val basedata = line._2._1
        val adddata = getsome(line._2._2)
        (id, basedata + "," + adddata)
      })
      .leftOuterJoin(investeerdd)
      .map(line => {
        val id = line._1
        val basedata = line._2._1
        val adddata = getsome(line._2._2)
        (id, basedata + "," + adddata)
      })
      .leftOuterJoin(changeinfordd)
      .map(line => {
        val id = line._1
        val basedata = line._2._1
        val adddata = getsome(line._2._2)
        (id, basedata + "," + adddata)
      })
      .leftOuterJoin(businessscoperdd)
      .map(line => {
        val id = line._1
        val basedata = line._2._1
        val adddata = getsome(line._2._2)
        id + "," + basedata + "," + adddata
      })
    rdd.saveAsTextFile(output)

    //      rdd.map(x=>(x.split(",")(1).split("\t")(5),x)).sortByKey(true)
    //     .map(_._2).saveAsTextFile(output1)

  }
}