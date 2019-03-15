package myscala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*
 * join name-id表，把所有name转成id
 */
object nametoid {
     @throws[Exception]
  def main(args: Array[String]):Unit={  

      val master = "yarn-client";
      val conf = new SparkConf().setAppName("nametoid").setMaster(master)  
      val sc = new SparkContext(conf)  

      
      //从hdfs读数据
      val inputFile1 = args(0)   //RDD1,branches,changeinfo
      /*
       * branches sample
caea6214c281315ac1165ef87eed26ec,上海齐薪汽车维护有限公司分公司
b2ae70d1ce799ef19ee7f712eb125650,上海佳速百货商行长宁分部
e51d1bd7f0d7c3548deb98c0430b03f9,韶关市乳峰物流有限公司
揭阳市金叶发展有限公司烟酒经营部,揭阳市金叶发展有限公司

       */
      /*
       * changeinfo sample
cf02b862b0f309da189cdffa7c2e9437,自然人股东	2008-03-27	2012-03-27	2012-03-27	自然人股东	2008-03-27	2012-03-27	2012-03-27
cd7c2133a87cf2a5ba996a8a8ecf9174,经营范围	2009-10-29	经营范围:食杂、酒、食用盐、水果、蔬菜、糖茶、卷烟	经营范围:食杂、酒、食用盐、水果、蔬菜、糖茶、卷烟、药品专柜(非处方药)、成品粮油、五金电料、服装布匹、零售
b328df0b5ca1a633b28026af226591f6,名称变更	2009-02-24	无字号	上海市嘉定区嘉定镇街道云蜜百货商店
       */
     val companynamemap = "/companyinfo/out/companynamemap"  //RDD2    
     //val inputFile2 = "rdd2.txt"  //RDD2  
      /* 
       * sample
bae0972123c94414e850885f1c0a2fe1,湖南南方物业总公司出租车分公司
1cf790e1f5eec8ac482371661f65fb78,望奎县昌友玉米种植农民专业合作社
d098eac7f78c07e89d38500e8da4556d,东莞市东城主山大井头物业管理有限公司
efe6c94c750ef4783933c9fba03fb46e,上海宏鸣实业有限公司第一化妆品分公司
       */
      val outputFile = args(1)  //完整的URI的路径名
      
//      val inputFile1="rdd1.txt"
//      val inputFile2="rdd2.txt"
      
      def getsome(x:Option[String])=x match{
          case Some(a)=>a
          case None =>""
      } 
      //数据应以id/name作为key，map应以名字为key
      val namemap=sc.textFile(companynamemap).map(_.split(",",2)).filter(_.size==2).map(x=>(x(1),x(0)))
      val rdd = sc.textFile(inputFile1).map(_.split("\t",2)).filter(_.size==2).map(x=>(x(0),x(1)))
      .leftOuterJoin(namemap)
      .map(line=>{
        val key=line._1
        val key1=getsome(line._2._2)
        val value=line._2._1
        if(key1.equals("")&&key.length()==32)
          key+","+value
        else if(key1.length()==32)
          key1+","+value
      }).distinct().saveAsTextFile(outputFile)
      
  }  
}