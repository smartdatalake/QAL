package mains

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.Date

import definition.ModelInfo
import definition.Paths._
import mains.density_cluster.sparkSession
import sparkSession.implicits._

import scala.collection.{Seq, mutable}
import scala.io.Source

object DynamicModelSelection extends QueryEngine_Abs("DynamicModelSelection") {

  parentDir = "/home/hamid/QAL/QP/skyServer/"
  pathToQueryLog = "/home/hamid/workloadFeatured"
  pathToTableParquet = parentDir + "data_parquet/"
  loadTables(sparkSession)
  val pathToML_Info = "/home/hamid/QAL/DensityCluster/"
  val tag = "_train_gap1800_processMinLength3_maxQueryRepetition10000_featureMinFRQ1_reserveFeature15"
  val tag2 = "_test_gap1800_processMinLength3_maxQueryRepetition10000_featureMinFRQ1_reserveFeature15"
  val url = "http://localhost:5000/vec?vec="
  val window = "10"
  val timeBucket = 14
  val k = 10
  val modelInfo: mutable.HashMap[(String, Int), ModelInfo] = new mutable.HashMap[(String, Int), ModelInfo]()
  val setup: Seq[String] = Seq("4YearTo2Week", "4YearTo6Month", "2WeekTo2Week", "1DayTo1Day")
  val sessions = new mutable.Queue[Seq[(String, Long, String)]]() // (ip,epoch,queryFeatures)
  ReadModelInfo()
  val m = modelInfo.toSeq.sortBy(_._1._2).reverse

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  def main(args: Array[String]): Unit = {
    val begin = new Date(2008, 0, 1, 0, 0, 0).getTime / 1000
    val end = new Date(2021, 0, 1, 0, 0, 0).getTime / 1000
    var F4Yto2W = 0
    var F4Yto6M = 0
    var F2Wto2W = 0
    var F1Dto1D = 0
    var Fbest = 0
    var FDynamic6M = 0
    var FDynamic2W = 0
    var counter = 0
    var counterT = 0
    var time: Long = 0
    var PreVal2Wto2W = 0
    var PreVal1Dto1D = 0
    var PreVal4Yto2W = 0
    var PreVal4Yto6M = 0
    var dynamicTrueSelection = 0
    var dynamicFalseSelection = 0
    val queryLog = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "false")
      .option("inferSchema", "true").option("delimiter", ";").option("nullValue", "null").schema(logSchema2).load(pathToQueryLog)
      .filter("yy >2019 and yy<2021").map(x => (x.getString(0), x.getLong(1), x.getString(8))).collect()
    var validationSessions = toSession(queryLog.take(1000)).sortBy(_ (0)._2).takeRight(k)
    var vectorsTest2Wto2W_m: ((String, Int), ModelInfo) = null
    var vectorsTest1Dto1D_m: ((String, Int), ModelInfo) = null
    var vectorsTest4Yto2W_m: ((String, Int), ModelInfo) = null
    var vectorsTest4Yto6M_m: ((String, Int), ModelInfo) = null
    var avoid2Wto2W = 0
    var avoid1Dto1D = 0
    var avoid4Yto2W = 0
    var avoid4Yto6M = 0
    var time2: Long = 0
    for (i <- begin + 6 * MONTH to end - 1 by DAY) {
      //println(counterT)
      val testSessions = toSession(queryLog.filter(query => i <= query._2 && query._2 < i + DAY))
      // if (testSessions.size > 0)
      //   println(testSessions.map(_.size).reduce(_ + _))
      val ttt = System.nanoTime()
      if (testSessions.size > 0) {
        if (PreVal4Yto2W < 80)
          vectorsTest4Yto2W_m = m.find(x => x._1._1.equalsIgnoreCase("4YearTo2Week") && x._1._2 <= ((testSessions(0)(0)._2 - begin) / (2 * WEEK)).toInt).get
        else avoid4Yto2W += 1
        if (PreVal4Yto6M < 80)
          vectorsTest4Yto6M_m = m.find(x => x._1._1.equalsIgnoreCase("4YearTo6Month") && x._1._2 <= ((testSessions(0)(0)._2 - begin) / (6 * MONTH)).toInt).get
        else avoid4Yto6M += 1
        var t = System.nanoTime()
        if (PreVal2Wto2W < 50)
          vectorsTest2Wto2W_m = m.find(x => x._1._1.equalsIgnoreCase("2WeekTo2Week") && x._1._2 <= ((testSessions(0)(0)._2 - begin) / (2 * WEEK)).toInt).get
        else avoid2Wto2W += 1
        if (PreVal1Dto1D < 50)
          vectorsTest1Dto1D_m = m.find(x => x._1._1.equalsIgnoreCase("1DayTo1Day") && x._1._2 <= ((testSessions(0)(0)._2 - begin) / (1 * DAY)).toInt).get
        else avoid1Dto1D += 1

        val vectorsVal4Yto2W = processesToVectors(validationSessions, vectorsTest4Yto2W_m._2)
        val vectorsVal4Yto6M = processesToVectors(validationSessions, vectorsTest4Yto6M_m._2)
        //val x = toSession(queryLog.filter(query => i - 4 * DAY <= query._2 && query._2 < i))
        val vectorsVal2Wto2W = processesToVectors((validationSessions), vectorsTest2Wto2W_m._2)
        val vectorsVal1Dto1D = processesToVectors(validationSessions.takeRight(3), vectorsTest1Dto1D_m._2)
        time += (System.nanoTime() - t)
        counterT += 1
        val vectorsTest4Yto2W = processesToVectors(testSessions, vectorsTest4Yto2W_m._2)
        val vectorsTest4Yto6M = processesToVectors(testSessions, vectorsTest4Yto6M_m._2)
        val vectorsTest2Wto2W = processesToVectors(testSessions, vectorsTest2Wto2W_m._2)
        val vectorsTest1Dto1D = processesToVectors(testSessions, vectorsTest1Dto1D_m._2)


        var temp = getResult(vectorsTest4Yto2W, vectorsTest4Yto2W_m._1._1, vectorsTest4Yto2W_m._1._2)
        val test4Yto2W = if (temp.size == 0) 0 else temp.toInt
        temp = getResult(vectorsTest4Yto6M, vectorsTest4Yto6M_m._1._1, vectorsTest4Yto6M_m._1._2)
        val test4Yto6M = if (temp.size == 0) 0 else temp.toInt
        temp = getResult(vectorsTest2Wto2W, vectorsTest2Wto2W_m._1._1, vectorsTest2Wto2W_m._1._2)
        val test2Wto2W = if (temp.size == 0) 0 else temp.toInt
        temp = getResult(vectorsTest1Dto1D, vectorsTest1Dto1D_m._1._1, vectorsTest1Dto1D_m._1._2)
        val test1Dto1D = if (temp.size == 0) 0 else temp.toInt
        t = System.nanoTime()
        temp = getResult(vectorsVal4Yto2W, vectorsTest4Yto2W_m._1._1, vectorsTest4Yto2W_m._1._2)
        val val4Yto2W = if (temp.size > 0) temp.toInt else 0
        temp = getResult(vectorsVal4Yto6M, vectorsTest4Yto6M_m._1._1, vectorsTest4Yto6M_m._1._2)
        val val4Yto6M = if (temp.size > 0) temp.toInt else 0
        temp = getResult(vectorsVal2Wto2W, vectorsTest2Wto2W_m._1._1, vectorsTest2Wto2W_m._1._2)
        val val2Wto2W = if (temp.size > 0) temp.toInt else 0
        temp = getResult(vectorsVal1Dto1D, vectorsTest1Dto1D_m._1._1, vectorsTest1Dto1D_m._1._2)
        val val1Dto1D = if (temp.size > 0) temp.toInt else 0
        time += (System.nanoTime() - t)
        PreVal2Wto2W = test2Wto2W
        PreVal1Dto1D = test1Dto1D
        PreVal4Yto2W = test4Yto2W
        PreVal4Yto6M = test4Yto6M

        if (test4Yto2W > -1) {
          println("Test4yto2w: " + test4Yto2W)
          println("Test4yto6m: " + test4Yto6M)
          println("Test2wto2w: " + test2Wto2W)
          println("Test1dto1d: " + test1Dto1D)
          println("xx: " + (test2Wto2W - test4Yto2W))
          println("yy: " + (val2Wto2W - val4Yto2W))
          println("val4yto2w: " + val4Yto2W)
          println("val4yto6m: " + val4Yto6M)
          println("val2wto2w: " + val2Wto2W)
          println("val1dto1d: " + val1Dto1D)
          println("-------------------------------------------------------------------------------")
          F4Yto2W += test4Yto2W
          F4Yto6M += test4Yto6M
          F2Wto2W += test2Wto2W
          F1Dto1D += test1Dto1D
          Fbest +=
            (if (test1Dto1D > test4Yto2W) test1Dto1D else if (test2Wto2W > test4Yto2W) test2Wto2W else test4Yto2W)

          if (test1Dto1D > test4Yto2W) {
            println("1D" + counter)
            println(test1Dto1D - test4Yto2W)
            println(val1Dto1D)
            println(val4Yto2W)
            //    queryLog.filter(query => i <= query._2 && query._2 < i + DAY).foreach(println)
            //    println("---------------------------------------------------------------------------")
          }
          if (test2Wto2W > test4Yto2W && !(test1Dto1D > test4Yto2W)) {
            println("2W" + counter)
            println(test2Wto2W - test4Yto2W)
            println(val2Wto2W)
            println(val4Yto2W)
          }


          if ((val2Wto2W >= 97) || (val2Wto2W - val4Yto2W) >= 35 || ((val2Wto2W - val4Yto2W) >= 15 && val2Wto2W >= 80))
            FDynamic2W += test2Wto2W
          else if (val1Dto1D >= 907 && (val1Dto1D > (val4Yto2W + 10)))
            FDynamic2W += test1Dto1D
          else
            FDynamic2W += test4Yto2W


          if (val2Wto2W >= 97 || (val2Wto2W - val4Yto6M) >= 35 || ((val2Wto2W - val4Yto6M) >= 15 && val2Wto2W >= 80)) {
            FDynamic6M += test2Wto2W
            if (test2Wto2W >= test4Yto6M)
              dynamicTrueSelection += 1
            else
              dynamicFalseSelection += 1
            // println(test2Wto2W)
          }
          else if (val1Dto1D > val4Yto6M + 10 && val1Dto1D >= 907) {
            FDynamic6M += test1Dto1D
            if (test1Dto1D >= test4Yto6M)
              dynamicTrueSelection += 1
            else
              dynamicFalseSelection += 1
            //println(test1Dto1D)
          }
          else {
            if (test4Yto6M >= test1Dto1D)
              dynamicTrueSelection += 1
            else
              dynamicFalseSelection += 1
            FDynamic6M += test4Yto6M
            // println(test4Yto6M)
          }
          counter += 1
        }
        validationSessions = /*testSessions  */ (testSessions ++ validationSessions.filter(_.size >= 5)).sortBy(_.head._2).takeRight(k)

      }
      time2 += (System.nanoTime() - ttt)
    }
    println("counter: " + counter)
    println("time: " + time / (1000000000).toDouble)
    println("time2: " + time2 / (1 * 1000000000).toDouble)
    println("dynamicTrueSelection: " + dynamicTrueSelection)
    println("dynamicFalseSelection: " + dynamicFalseSelection)
    println("best: " + Fbest / counter.toDouble)
    println("4Yto2W: " + F4Yto2W / counter.toDouble)
    println("Dynamic2W: " + FDynamic2W / counter.toDouble)
    println("Dynamic6M: " + FDynamic6M / counter.toDouble)
    println("4Yto6M: " + F4Yto6M / counter.toDouble)
    println("2Wto2W: " + F2Wto2W / counter.toDouble)
    println("1DayTo1Day: " + F1Dto1D / counter.toDouble)
    println("avoid6M: " + avoid4Yto6M)
    println("avoid42W: " + avoid4Yto2W)
    println("avoid1D: " + avoid1Dto1D)
    println("avoid2W: " + avoid2Wto2W)
  }


  def ReadModelInfo() = {
    for (s <- setup) {
      for (m <- 1 to 5000) if (Files.exists(Paths.get(pathToML_Info + s + "_M" + m + tag + "_colIndex"))) {
        val model = new ModelInfo(s, m)
        var lines = Source.fromFile(pathToML_Info + s + "_M" + m + tag + "_colIndex").getLines
        while (lines.hasNext) {
          val entry = (lines.next()).split(delimiterHashMap)
          model.indexToAccessedCol.put(entry(1).toInt, entry(0))
          model.accessedColToVectorIndex.put(entry(0), entry(1).toInt)
        }
        lines = Source.fromFile(pathToML_Info + s + "_M" + m + tag + "_groupIndex").getLines
        while (lines.hasNext) {
          val entry = (lines.next()).split(delimiterHashMap)
          model.indexToGroupByKey.put(entry(1).toInt, entry(0))
          model.groupByKeyToVectorIndex.put(entry(0), entry(1).toInt)
        }
        lines = Source.fromFile(pathToML_Info + s + "_M" + m + tag + "_joinIndex").getLines
        while (lines.hasNext) {
          val entry = (lines.next()).split(delimiterHashMap)
          model.indexToJoinKey.put(entry(1).toInt, entry(0))
          model.joinKeyToVectorIndex.put(entry(0), entry(1).toInt)
        }
        lines = Source.fromFile(pathToML_Info + s + "_M" + m + tag + "_tableIndex").getLines
        while (lines.hasNext) {
          val entry = (lines.next()).split(delimiterHashMap)
          model.vectorSize = entry(1).toInt
          model.indexToTable.put(entry(1).toInt, entry(0))
          model.tableToVectorIndex.put(entry(0), entry(1).toInt)
        }
        model.vectorSize += (1 + timeBucket)
        var flag = false
        lines = Source.fromFile(pathToML_Info + s + "_M" + m + tag).getLines
        while (lines.hasNext)
          if (lines.next().split(";").size >= 5)
            flag = true
        var flag2 = false
        lines = Source.fromFile(pathToML_Info + s + "_M" + m + tag2).getLines
        while (lines.hasNext)
          if (lines.next().split(";").size >= 5)
            flag2 = true
        if (model.isValid() && flag && flag2)
          modelInfo.put((s, m), model)
      }
    }
  }

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def getResult(processes: String, tag: String, m: Int): String = {
    System.in.close()
    scala.io.Source.fromURL(url + tag + "X" + m + "X" + processes).mkString
  }

  override def ReadNextQueries(query: String, ip: String, epoch: Long, queryIndex: Int): Seq[String] = null

  override def readConfiguration(args: Array[String]) = {}

}

