package myscala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*
 * 生成被投资企业表，逻辑上好乱。。。
 */
object getinvestee {
  @throws[Exception]
  def main(args: Array[String]): Unit = {

    val master = "yarn-client";
    val conf = new SparkConf().setAppName("getinvestee").setMaster(master)
    val sc = new SparkContext(conf)

    //从hdfs读数据
    // /companyinfo/logosdata/partner
    val partnerdata = "/companyinfo/out/sizematched/partner" //RDD1,partner
    /*
       * partner sample
重庆镇八方餐饮文化传播有限公司	唐邦玮	自然人	60		30.0%
重庆镇八方餐饮文化传播有限公司	汪丹	自然人	30		15.00%
台州迪欧咖啡餐饮有限公司	王阳发	自然人股东	10万元	2003-09-05	100%
湛江市赤坎科教仪器家电商场	湛江市赤坎区寸金工商业总公司	-	-		100%
北京富贵百全商贸有限公司	朱杰	自然人股东	-		-
北京富贵百全商贸有限公司	王铁	自然人股东	-		-
菏泽市牡丹区震鲁废旧物资回收有限公司	王震	自然人股东	50	2013年05月21日	100%
       */
    val companynamemapdata = "/companyinfo/out/companynamemap" //RDD2
    /*
       * sample
bae0972123c94414e850885f1c0a2fe1,湖南南方物业总公司出租车分公司
1cf790e1f5eec8ac482371661f65fb78,望奎县昌友玉米种植农民专业合作社
d098eac7f78c07e89d38500e8da4556d,东莞市东城主山大井头物业管理有限公司
efe6c94c750ef4783933c9fba03fb46e,上海宏鸣实业有限公司第一化妆品分公司
       */
    //为投资类别中除了个人之外的类别
    val companytypedata = "/companyinfo/companytype.txt"
    //baseinfo
    val baseinfodata = "/companyinfo/out/sizematched/baseinfo"
    val outputFile = "/companyinfo/out/combined/investee"

    def getsome(x: Option[String]) = x match {
      case Some(a) => a
      case None    => ""
    }
    //数据应以切分1作为key，map应以名字为key
    //被投资企业名称1	被投资法定代表人1	注册资本1	投资数额1	投资占比1	注册时间1	状态1
    //B的股东是A
    //partner idB：投资数额	投资占比 nameA
    //baseinfo idB：被投资企业名称	被投资法定代表人	注册资本 注册时间	状态
    //合并为 nameA :nameB	B法人	B注册资本 B注册时间	B状态 投资数额	投资占比
    //nameA转ID

    //(股东类型（公司）,)
    val companypartner = sc.textFile(companytypedata)
      .map(_.split("\t",-1)).filter(_.size == 2).map(x => (x(0), x(1)))
    //(name,id)
    val companynamemap = sc.textFile(companynamemapdata)
      .map(_.split(",", 2))
      .filter(_.size == 2).map(x => (x(1), x(0)))
    val baseinfo = sc.textFile(baseinfodata).map(_.split("\t",-1))
      .filter(_.size == 20)
      .map(x => (x(0), x(1) + "\t" + x(3) + "\t" + x(4) + "\t" + x(5) + "\t" + x(6) + "\t" + x(15)))

    val nameinfo = sc.textFile(partnerdata)
      .map(_.split("\t",-1)).filter(_.size == 6)
      //(股东类型,idB/nameB idA/nameA 认缴出资额 持股比例)
      .map(x => (x(2), x(0) + "\t" + x(1) + "\t" + x(3) + "\t" + x(5)))
      //.partitionBy(new org.apache.spark.HashPartitioner(500))
      //提取公司股东
      .filter(x => !(x._1.equals("自然人股东") || x._1.equals("境内中国公民") || x._1.equals("自然人") || x._1.equals("非农民自然人")))
      .join(companypartner)
      //(idB/nameB,idA/nameA 投资数额 投资占比)
      .map(_._2._1.split("\t",-1))
      .filter(_.size == 4)
      .map(x => (x(0), x(1) + "\t" + x(2) + "\t" + x(3)))
      //公司名字转id
      //(idB,idA/nameA 投资数额 投资占比)
      .leftOuterJoin(companynamemap)
      .map(line => {
        val id = line._1
        val id1 = getsome(line._2._2)
        val value = line._2._1
        if (id1.equals(""))
          (id, value)
        else
          (id1, value)
      })
      //(idB,公司名 法人 注册资金 成立日期 经营状态 核准日期)
      .join(baseinfo)
      //(idA/nameA,投资数额 投资占比 公司名 法人 注册资金 成立日期 经营状态 核准日期)
      .map(x => {
        val partnerdata = x._2._1.split("\t", 2)
        val basedata = x._2._2
        (partnerdata(0), partnerdata(1) + "\t" + basedata)
      })
      //投资公司名字转id
      //idA,投资数额 投资占比 公司名 法人 注册资金 成立日期 经营状态 核准日期
      .leftOuterJoin(companynamemap)
      .map(line => {
        val key = line._1
        val key1 = getsome(line._2._2)
        val value = line._2._1
        if (key1.equals("")  && key.length == 32)
          (key, value)
        else if (key1.length() == 32)
          (key1, value)
        else
          ("", "")
      }).distinct()
      //多行合并为一行
      .filter(!_._1.equals(""))
      .reduceByKey(_ + "\t" + _)
      .map(line => {
        val key = line._1
        val value = line._2
        key + "," + value
      }).saveAsTextFile(outputFile)
  }
}