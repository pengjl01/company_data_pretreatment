package myscala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.{ Try, Success, Failure }
import util.control.Breaks._
import java.text.ParseException
import java.io.PrintWriter
import java.io.File

/*
 * step1
 * 统计字段
 */
object newtable {
  val writer = new PrintWriter(new File("./newtableout.txt"))
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val master = "yarn-client";
    val conf = new SparkConf().setAppName("newtable").setMaster(master)
    val sc = new SparkContext(conf)

    easymode(sc)
    maketypenum(sc)
    hardmode(sc)
    getnewtable(sc)

    //    countfirstpartner(sc)
    //    get(sc)
    writer.close()
  }
  def easymode(sc: SparkContext): Unit = {
    val inputFile1 = "/companyinfo/outtxt" //输入文件
    val typerdd1 = sc.textFile("/companyinfo/newtable/typewaizi.txt").map((_, "1")).distinct()
    val typerdd2 = sc.textFile("/companyinfo/newtable/typeguoqi.txt").map((_, "2")).distinct()
    val typerdd3 = sc.textFile("/companyinfo/newtable/typesiying.txt").map((_, "3")).distinct()
    val typerdd4 = sc.textFile("/companyinfo/newtable/typehezi.txt").map((_, "4")).distinct()
    val typerdd5 = sc.textFile("/companyinfo/newtable/typezigongsi.txt").map((_, "5")).distinct()
    val typerdd6 = sc.textFile("/companyinfo/newtable/typeshangshi.txt").map((_, "6")).distinct()
    val typerdd7 = sc.textFile("/companyinfo/newtable/typegetihu.txt").map((_, "7")).distinct()
    val typeall = typerdd1.union(typerdd2).union(typerdd3).union(typerdd4).union(typerdd5).union(typerdd6).union(typerdd7)
    val rdd = sc.textFile(inputFile1).map(_.split("\t"))
      //(企业类型,(注册时间,(城市,行业1-10)))
      .map(x => (x(18), (x(6), (x(26), x(29) + "\t" + x(30) + "\t" + x(31) + "\t" + x(32) + "\t" + x(33) + "\t" + x(34) + "\t" + x(35) + "\t" + x(36) + "\t" + x(37) + "\t" + x(38)))))
      .join(typeall)
      .map(x =>
        {
          val year = x._2._1._1.split("/|-", -1)(0)
          var intyear = 0;
          scala.util.Try(intyear = year.toInt)
          //(年int,((城市,行业1-10),企业类型int))
          (intyear, (x._2._1._2, x._2._2))
        })

    writer.println("easymode")
    dealtype(sc, rdd, "/companyinfo/newtable/out/data/easydata")
    val a = rdd.count
    writer.println("   easycount " + a)

//easymode
//   count4 21843972
//   count5 20948770
//   count6 1166546
//   easycount 38405807


  }
  //外资1 waizi，国企2 guoqi，私营3 siying，合资4 hezi，子公司5 zigongsi，上市6 shangshi，个体户7 getihu
  def dealtype(sc: org.apache.spark.SparkContext, rdd: org.apache.spark.rdd.RDD[(Int, ((String, String), String))], output: String): Unit = {
    val citysFile = "/companyinfo/newtable/city.txt" //输入文件
    val cityskeys = sc.textFile(citysFile).map(_.split("\t", -1)).filter(_.length == 2)
      .map(x => (x(0), x(1))).distinct().collect()
    //(年int,((城市,行业1-10),企业类型))
    val datardd3 = rdd.filter(x => x._1 >= 1940 && x._1 <= 2017)
      .map(x => {
        val year = String.valueOf(x._1)
        val city = x._2._1._1
        val city1 = myfind(city, cityskeys)
        if (city1.equals("-"))
          ((("", year), x._2._2), x._2._1._2)
        else
          //(((城市,城市类别      ,   年 ),企业类型)     ,    行业1-10)
          (((city1, year), x._2._2), x._2._1._2)
      }).filter(!_._1._1._1.equals(""))
    val datardd4 = datardd3
      //(((城市,城市类别      ,   年 ),企业类型)    ,    行业)
      .flatMapValues(_.split("\t")).filter(!_._2.equals("-"))
      .filter(x => x._2.length == 3 || x._2.length == 4)
      //((城市,城市类别,行业,年/t企业类型),1)
      .map(x => {
        if (x._2.length == 4)
          (x._1._1._1 + "," + x._2.substring(0, 3) + "," + x._1._1._2 + "\t" + x._1._2, 1)
        else
          (x._1._1._1 + "," + x._2 + "," + x._1._1._2 + "\t" + x._1._2, 1)
      })
    val datardd5 = datardd4.reduceByKey(_ + _)
      //(城市,城市类别,行业,年/t 企业类型/t数量)
      .map(x => x._1 + "\t" + String.valueOf(x._2))
    datardd5.saveAsTextFile(output)

    val count4 = datardd3.count
    val count5 = datardd4.count
    val count6 = datardd5.count
    writer.println("   count4 " + count4)
    writer.println("   count5 " + count5)
    writer.println("   count6 " + count6)
  }

  def maketypenum(sc: SparkContext): Unit = {
    val inputFile1 = "/companyinfo/outtxt" //输入文件
    val typeFile = "/companyinfo/newtable/typeweizhi.txt"
    val outputtypenum = "/companyinfo/newtable/out/typenum"
    //外资1 waizi，国企2 guoqi，私营3 siying，合资4 hezi，子公司5 zigongsi，上市6 shangshi，个体户7 getihu
    val typerdd = sc.textFile(typeFile).map((_, 8)).distinct()
    val typerdd1 = sc.textFile("/companyinfo/newtable/typewaizi.txt").map((_, 1)).distinct()
    val typerdd2 = sc.textFile("/companyinfo/newtable/typeguoqi.txt").map((_, 2)).distinct()
    val typerdd3 = sc.textFile("/companyinfo/newtable/typesiying.txt").map((_, 3)).distinct()
    val typerdd4 = sc.textFile("/companyinfo/newtable/typehezi.txt").map((_, 4)).distinct()
    val typerdd5 = sc.textFile("/companyinfo/newtable/typezigongsi.txt").map((_, 5)).distinct()
    val typerdd6 = sc.textFile("/companyinfo/newtable/typeshangshi.txt").map((_, 6)).distinct()
    val typerdd7 = sc.textFile("/companyinfo/newtable/typegetihu.txt").map((_, 7)).distinct()
    val typeall = typerdd.union(typerdd1).union(typerdd2).union(typerdd3).union(typerdd4).union(typerdd5).union(typerdd6).union(typerdd7).reduceByKey((a, b) => a)

    val nametype = sc.textFile(inputFile1).map(_.split("\t")).map(x => (x(18), x(1))).join(typeall).map(x => (x._2))

    val rdd = sc.textFile(inputFile1).map(_.split("\t"))
      //    (企业类型,（股东数，（最大股东比例，（股东1-10，id）））)
      .map(x => (x(18), (x(39), (x(41), (x(40) + "\t" + x(43) + "\t" + x(46) + "\t" + x(49) + "\t" + x(52) + "\t" + x(55) + "\t" + x(58) + "\t" + x(61) + "\t" + x(64) + "\t" + x(67), x(0))))))
      .join(typerdd)
      .map(x => {
        //（股东数，（最大股东比例，（股东1-10，id）））
        (x._2._1._1, x._2._1._2)
      }).filter(!_._1.equals("0")).distinct()
    val zigongsirdd = rdd.filter(_._1.equals("1"))
      //    (股东1,id)
      .map(x => (x._2._2._1.split("\t")(0), x._2._2._2))
      .join(nametype)
      //      (id,股东类型int)
      .map(line => {
        (line._2._1, line._2._2)
      }).filter(x => x._2 != 8 && x._2 != 7)
      .map(x => (x._1, 5)).distinct()
    val guoqirdd = rdd
      //    (股东1,(id,最大股东比例))
      .map(x => (x._2._2._1.split("\t")(0), (x._2._2._2, x._2._1)))
      .join(nametype)
      .map(x => {
        var biggestper = 0.0
        try {
          biggestper = x._2._1._2.split("%", -1)(0).toDouble
        } catch {
          case _ => None
        }
        val id = x._2._1._1
        val typenum = x._2._2
        (id, typenum, biggestper)
      }).filter(x => x._2 == 2 && x._3 > 50)
      .map(x => (x._1, 2)).distinct()
    val waizihezirdd = rdd
      //    (id,股东1-10)
      .map(x => (x._2._2._2, x._2._2._1))
      //      (股东，id)
      .flatMapValues(_.split("\t")).filter(!_._2.equals("-")).map(x => (x._2, x._1))
      .leftOuterJoin(nametype)
      //      (id，股东type)
      .map(line => {
        val partnername = line._1
        val typ = getsome(line._2._2)
        val id = line._2._1
        (id, typ.toString)
      })
      .reduceByKey(_ + "\t" + _)
      .map(x => {
        val id = x._1
        val partnertypes = x._2.split("\t")
        var typenum = 8
        var waiqinum = 0
        var feiwaiqinum = 0
        for (i <- 0 until partnertypes.length) {
          if (partnertypes(i).equals("1"))
            waiqinum += 1
          else if (!partnertypes(i).equals("8"))
            feiwaiqinum += 1
        }
        if (waiqinum == partnertypes.length)
          typenum = 1
        else if (feiwaiqinum >= 1 && waiqinum >= 1)
          typenum = 4
        (id, typenum)
      }).filter(_._2 != 8).distinct()

    val typenum = zigongsirdd.union(guoqirdd).union(waizihezirdd).reduceByKey((a, b) => a).map(x => x._1 + "," + x._2.toString()).saveAsTextFile(outputtypenum)
    val conut1 = zigongsirdd.count
    val conut2 = guoqirdd.count
    val conut3 = waizihezirdd.count

    writer.println("maketypenum")
    writer.println("   zigongsirdd " + conut1)
    writer.println("   guoqirdd " + conut2)
    writer.println("   waizihezirdd " + conut3)
//maketypenum
//   zigongsirdd 51345
//   guoqirdd 55028
//   waizihezirdd 12957


  }
  def hardmode(sc: SparkContext): Unit = {
    val inputFile1 = "/companyinfo/outtxt" //输入文件
    val inputFile2 = "/companyinfo/newtable/out/typenum" //输入文件
    val citysFile = "/companyinfo/newtable/city.txt" //输入文件
    val typeFile = "/companyinfo/newtable/typeweizhi.txt"
    val output = "/companyinfo/newtable/out/data/harddata"

    val typerdd = sc.textFile(typeFile).map((_, 8)).reduceByKey((a, b) => a)
    val cityskeys = sc.textFile(citysFile).map(_.split("\t", -1)).filter(_.length == 2)
      .map(x => (x(0), x(1))).reduceByKey((a, b) => a).collect()
    val typenumrdd = sc.textFile(inputFile2).map(_.split(",")).map(x => (x(0), x(1))).reduceByKey((a, b) => a)
    //(注册时间,(城市,行业1-10)
    val datardd1 = sc.textFile(inputFile1).map(_.split("\t"))
      //(企业类型,(id,(注册时间,(城市,行业1-10))))
      .map(x => (x(18), (x(0), (x(6), (x(26), x(29) + "\t" + x(30) + "\t" + x(31) + "\t" + x(32) + "\t" + x(33) + "\t" + x(34) + "\t" + x(35) + "\t" + x(36) + "\t" + x(37) + "\t" + x(38))))))
      .join(typerdd)
      //      (id,(注册时间,(城市,行业1-10)))
      .map(x => x._2._1)
    //      hard总数

    def gettype(x: Option[String]) = x match {
      case Some(a) => a
      case None    => "8"
    }
    val datardd2 = datardd1.leftOuterJoin(typenumrdd).reduceByKey((a, b) => a)
      .map(x => {
        val year = x._2._1._1.split("/|-", -1)(0)
        var intyear = 0;
        scala.util.Try(intyear = year.toInt)
        //(年int,((城市,行业1-10),企业类型))
        (intyear, (x._2._1._2, gettype(x._2._2)))
      })

    writer.println("hardmode")
    //(年int,((城市,行业1-10),企业类型))
    dealtype(sc, datardd2, "/companyinfo/newtable/out/data/harddata")
    val count2 = datardd1.count
    writer.println("   count2 " + count2)

//hardmode
//   count4 17252262
//   count5 34951049
//   count6 719651
//   count2 23625803


  }
  def myfind(value: String, keys: Array[(String, String)]): String = {
    var ans = "-"
    breakable {
      for (i <- 0 until keys.length) {
        val key = keys(i)._1
        if (key.indexOf(value) != -1) {
          ans = keys(i)._1 + "," + keys(i)._2
          break()
        }
      }
    }
    ans
  }

  def getnewtable(sc: SparkContext): Unit = {
    //保定,三线城市,302,2015	7	27
    val inputFile1 = "/companyinfo/newtable/out/data/*/part*" //输入文件
    val output = "/companyinfo/newtable/newtablefinal" //输出文件
    val output1 = "/companyinfo/newtable/newtablefinal1" //输出文件
    val output2 = "/companyinfo/newtable/newtablefinalbase" //输出文件

    val inputFile2 = "/companyinfo/newtable/city.txt" //输入文件
    val inputFile3 = "/companyinfo/newtable/basehangye.txt" //输入文件
    val inputFile4 = "/companyinfo/newtable/basenianfen.txt" //输入文件

    var r1 = sc.textFile(inputFile2).map(_.split("\t", -1)).filter(_.length == 2)
      .map(x => x(0) + "," + x(1)).distinct()
    var r2 = sc.textFile(inputFile3).filter(x => x.length == 3 || x.length == 4)
      .map(x => {
        if (x.length == 4)
          x.substring(0, 3)
        else
          x
      })
    var r3 = sc.textFile(inputFile4).distinct()

    val base = ((r1 cartesian r2).map(x => x._1 + "," + x._2) cartesian r3)
      .map(x => (x._1 + "," + x._2, "")).distinct()
      .sortByKey(ascending = true)
    base.saveAsTextFile(output2)
    val rdd = sc.textFile(inputFile1).map(_.split("\t")).filter(_.length == 3)
      .map(x => {
        val key = x(0)
        val index = x(1).toInt
        val count = x(2).toInt
        var data: Array[Int] = Array(count, 0, 0, 0, 0, 0, 0, 0, 0)
        data(index) = count
        (key, data)
      })
      .reduceByKey((a, b) => (a, b).zipped.map(_ + _))

    rdd.sortByKey(ascending = true)
      .map(x => {
        x._1 + "," + x._2(0) + "," + x._2(1) + "," + x._2(2) + "," + x._2(3) + "," + x._2(4) + "," + x._2(5) + "," + x._2(6) + "," + x._2(7) + "," + x._2(8)
      })
      .saveAsTextFile(output)
    rdd.rightOuterJoin(base)
      .map(line => {
        val id = line._1
        val value = getsome(line._2._1)
        (id, value)
      }).sortByKey(ascending = true)
      .map(x => {
        x._1 + "," + x._2(0) + "," + x._2(1) + "," + x._2(2) + "," + x._2(3) + "," + x._2(4) + "," + x._2(5) + "," + x._2(6) + "," + x._2(7) + "," + x._2(8)
      })
      .saveAsTextFile(output1)
  }

  def countfirstpartner(sc: SparkContext): Unit = {
    //40+3
    val inputFile1 = "/companyinfo/outtxt" //输入文件
    val companynamemap = "/companyinfo/out/companynamemap" //RDD2
    val citysFile = "/companyinfo/newtable/city.txt" //输入文件
    val typeFile = "/companyinfo/newtable/typeweizhi.txt"
    val output = "/companyinfo/newtable/out/data2/"

    val namemap = sc.textFile(companynamemap).map(_.split(",", 2)).filter(_.size == 2).map(x => (x(1), x(0)))
    val typerdd = sc.textFile(typeFile).map((_, "")).distinct()
    val rdd = sc.textFile(inputFile1).map(_.split("\t"))
      //(企业类型，(id,股东1-10))
      .map(x => (x(18), (x(0), x(40) + "," + x(43) + "," + x(46) + "," + x(49) + "," + x(52) + "," + x(55) + "," + x(58) + "," + x(61) + "," + x(64) + "," + x(67))))
      .join(typerdd).map(_._2._1)
    //    val allcompany = rdd.count
    //    //(id，股东)
    //    val rdd2 = rdd.flatMapValues(_.split(","))
    //      .filter(!_._2.equals("-"))
    //    val allpartner = rdd2.count
    //    //（股东，id）变为（股东，（id，股东id））
    //    var rdd1 = rdd2.map(x => (x._2, x._1)).join(namemap)
    //    val joinedpartner = rdd1.count
    //    val joinedcompany = rdd1.map(x => x._2._1).distinct().count

    val rddfirst = rdd.map(x => {
      val id = x._1
      val first = x._2.split(",", 2)(0)
      val other = x._2.split(",", 2)(1)
      //（第一个股东，（id，2-9股东））
      (first, (id, other))
    }).leftOuterJoin(namemap)

    val firstiscompany = rddfirst.count
    val firstnotcompany2 = rddfirst.count
    //    println("allcompany " + allcompany)
    //    println("allpartner " + allpartner)
    //    println("joinedpartner " + joinedpartner)
    //    println("joinedcompany " + joinedcompany)
    println("firstiscompany " + firstiscompany)
    println("firstnotcompany2 " + firstnotcompany2)
  }
  def getsome(x: Option[Array[Int]]) = x match {
    case Some(a) => a
    case None    => Array(0, 0, 0, 0, 0, 0, 0, 0, 0)
  }
  def getsome(x: Option[String]) = x match {
    case Some(a) => a
    case None    => ""
  }
  def getsome(x: Option[Int]) = x match {
    case Some(a) => a
    case None    => 8
  }

  def get(sc: SparkContext): Unit = {
    val output = "/companyinfo/newtable/newtablefinal" //输出文件
    val rdd = sc.textFile(output).map(_.split(",", -1)).map(x => (x(4).toInt, x(12).toInt)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    println(rdd._1 + " " + rdd._2)
    println(rdd._1 + " " + rdd._2)
    println(rdd._1 + " " + rdd._2)
    println(rdd._1 + " " + rdd._2)
    println(rdd._1 + " " + rdd._2)
    println(rdd._1 + " " + rdd._2)
  }
}