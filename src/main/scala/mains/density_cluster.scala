package mains

import java.io.{File, PrintWriter}
import java.util.Date

import definition.Paths._
import mains.density_cluster.sparkSession
import scalax.chart.api.ChartPNGExporter
import scalax.chart.module.XYChartFactories.XYLineChart
import sparkSession.implicits._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object density_cluster extends QueryEngine_Abs("DensityCluster") {


  val tag = (gap + "Gap_" + minProcessLength + "processMinLength_" + MAX_NUMBER_OF_QUERY_REPETITION + "maxQueryRepetition_"
    + ACCESSED_COL_MIN_FREQUENCY + "featureMinFRQ_" + reserveFeature + "reserveFeature_" + YEAR_FROM + "fromYear")
  val tagTest = ("test_" + gap + "Gap_" + minProcessLength + "processMinLength_" + MAX_NUMBER_OF_QUERY_REPETITION + "maxQueryRepetition_"
    + ACCESSED_COL_MIN_FREQUENCY + "featureMinFRQ_" + reserveFeature + "reserveFeature_" + YEAR_FROM + "fromYear")
  var resultPath = ""
  var resultPathTest = ""
  var resultWordPath = ""
  var resultInfoPath = ""
  var chartPath = ""
  var pathToQueryLogTest = ""

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  def main(args: Array[String]): Unit = {
    readConfiguration(args)
    loadTables(sparkSession)

    val processesTrain = new ListBuffer[ListBuffer[(String, Int, Int, Int, Int, Int, Int, Long, String)]]
    val processesTest = new ListBuffer[ListBuffer[(String, Int, Int, Int, Int, Int, Int, Long, String)]]
    val processFRQ = new mutable.HashMap[Int, Int]
    val queryLog = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "false")
      .option("inferSchema", "true").option("delimiter", ";").option("nullValue", "null").schema(logSchema).load(pathToQueryLog)
    val tuples = queryLog.filter("statement is not null and clientIP is not null")
      .filter(row => row.getAs[String]("statement").trim.length > 0 && row.getAs[String]("clientIP").trim.length > 0
        && row.getAs[Int]("yy") >= YEAR_FROM)
     // .sort(col("clientIP").asc, col("yy").asc, col("mm").asc, col("dd").asc, col("hh").asc, col("mi").asc, col("ss").asc, col("seq").asc)
      .select("clientIP", "yy", "mm", "dd", "hh", "mi", "ss", "statement")
      .map(x => (x.getAs[String](0), x.getAs[Int](1), x.getAs[Int](2), x.getAs[Int](3), x.getAs[Int](4)
        , x.getAs[Int](5), x.getAs[Int](6), new Date(x.getAs[Int](1), x.getAs[Int](2), x.getAs[Int](3)
        , x.getAs[Int](4), x.getAs[Int](5), x.getAs[Int](6)).getTime / 1000, x.getAs[String](7)))
      .collect()
    var temp = new ListBuffer[(String, Int, Int, Int, Int, Int, Int, Long, String)]
    temp.+=(tuples(0))
    for (i <- 1 to tuples.size - 1)
      if (tuples(i)._1.equals(tuples(i - 1)._1) && 0 <= tuples(i)._8 - tuples(i - 1)._8 && tuples(i)._8 - tuples(i - 1)._8 <= gap)
        temp.+=(tuples(i))
      else {
        if (temp.size >= minProcessLength)
          processesTrain.+=(temp)
        temp = new ListBuffer[(String, Int, Int, Int, Int, Int, Int, Long, String)]
        temp.+=(tuples(i))
      }

    if(temp.size>0)
      processesTrain.+=(temp)
    val tuplesTest = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "false")
      .option("inferSchema", "true").option("delimiter", ";").option("nullValue", "null").schema(logSchema).load(pathToQueryLogTest).filter("statement is not null and clientIP is not null")
      .filter(row => row.getAs[String]("statement").trim.length > 0 && row.getAs[String]("clientIP").trim.length > 0
        && row.getAs[Int]("yy") >= YEAR_FROM)
    //  .sort(col("clientIP").asc, col("yy").asc, col("mm").asc, col("dd").asc, col("hh").asc, col("mi").asc, col("ss").asc, col("seq").asc)
      .select("clientIP", "yy", "mm", "dd", "hh", "mi", "ss", "statement")
      .map(x => (x.getAs[String](0), x.getAs[Int](1), x.getAs[Int](2), x.getAs[Int](3), x.getAs[Int](4)
        , x.getAs[Int](5), x.getAs[Int](6), new Date(x.getAs[Int](1), x.getAs[Int](2), x.getAs[Int](3)
        , x.getAs[Int](4), x.getAs[Int](5), x.getAs[Int](6)).getTime / 1000, x.getAs[String](7)))
      .collect()
    temp = new ListBuffer[(String, Int, Int, Int, Int, Int, Int, Long, String)]
    temp.+=(tuplesTest(0))
    for (i <- 1 to tuplesTest.size - 1)
      if (tuplesTest(i)._1.equals(tuplesTest(i - 1)._1) && 0 <= tuplesTest(i)._8 - tuplesTest(i - 1)._8 && tuplesTest(i)._8 - tuplesTest(i - 1)._8 <= gap)
        temp.+=(tuplesTest(i))
      else {
        if (temp.size >= minProcessLength)
          processesTest.+=(temp)
        temp = new ListBuffer[(String, Int, Int, Int, Int, Int, Int, Long, String)]
        temp.+=(tuplesTest(i))
      }
    if(temp.size>0)
      processesTest.+=(temp)
    val (processesVec, processesVecTest, accessedColToVectorIndex, groupByKeyToVectorIndex, joinKeyToVectorIndex, tableToVectorIndex
    , accessedColFRQ, groupByFRQ, joinKeyFRQ, tableFRQ, vec2FeatureAndFRQ) = processToVector(processesTrain, processesTest, sparkSession)
    var writer = new PrintWriter(new File(resultPath))
    processesVec.foreach(processInVec => writer.println(reduceVectorRepetition(processInVec)))
    writer.close()
    writer = new PrintWriter(new File(resultPathTest))
    processesVecTest.foreach(processInVec => writer.println(reduceVectorRepetition(processInVec)))
    writer.close()
    vec2word(vec2FeatureAndFRQ)
    processesTrain.map(x => (x(0)._1, x.map(tuple => tuple._9)))
    processesTrain.foreach(x => processFRQ.put(x.size, processFRQ.getOrElse(x.size, 0) + 1))
    writer = new PrintWriter(new File(resultPath + "_colIndex"))
    accessedColToVectorIndex.toList.sortBy(_._2).foreach(x => writer.println(x._1 + delimiterHashMap + x._2))
    writer.close()
    writer = new PrintWriter(new File(resultPath + "_groupIndex"))
    groupByKeyToVectorIndex.toList.sortBy(_._2).foreach(x => writer.println(x._1 + delimiterHashMap + x._2))
    writer.close()
    writer = new PrintWriter(new File(resultPath + "_joinIndex"))
    joinKeyToVectorIndex.toList.sortBy(_._2).foreach(x => writer.println(x._1 + delimiterHashMap + x._2))
    writer.close()
    writer = new PrintWriter(new File(resultPath + "_tableIndex"))
    tableToVectorIndex.toList.sortBy(_._2).foreach(x => writer.println(x._1 + delimiterHashMap + x._2))
    writer.close()
    writer = new PrintWriter(new File(resultPath + "_featureFRQ"))
    (accessedColFRQ.toList ++ groupByFRQ.toList ++ joinKeyFRQ.toList ++ tableFRQ.toList).sortBy(_._2).foreach(x => writer.println(x._1 + delimiterHashMap + x._2))
    writer.close()
    writer = new PrintWriter(new File(resultPath + "_vec2Feature"))
    (vec2FeatureAndFRQ).toList.foreach(x => writer.println(x._1 + delimiterHashMap + x._2))
    writer.close()
    writer = new PrintWriter(new File(resultPath + "_processFRQ"))
    processFRQ.toList.sortBy(_._1).map(x => (x._1, x._2)).foreach(writer.println)
    writer.close()
    XYLineChart(processFRQ.toList.sortBy(_._1).map(x => (x._1, x._2)).take(100)).saveAsPNG(chartPath)
    println("done")
  }

  def vec2word(vec2Feature: mutable.HashMap[String, (String, Long)]) = {
    val vec2FeatureAndWordAndFRQ = new mutable.HashMap[String, (String, String, Long)]()
    var index = 1
    sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "false")
      .option("inferSchema", "true").option("delimiter", "%").option("nullValue", "null").load(resultPath)
      .flatMap(x => x.getAs[String](0).split(";")).distinct().collect().toList.foreach(vec => {
      vec2FeatureAndWordAndFRQ.put(vec, (vec2Feature(vec)._1, "Q" + index, vec2Feature(vec)._2))
      index += 1
    })
    var writer = new PrintWriter(new File(resultWordPath))
    sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "false")
      .option("inferSchema", "true").option("delimiter", "%").option("nullValue", "null").load(resultPath)
      .map(row => row.getAs[String](0).split(";").map(vec2FeatureAndWordAndFRQ.get(_).get._2).mkString(" ")).collect().foreach(process => writer.println(process))
    writer.close()

    writer = new PrintWriter(new File(resultWordPath + "_vec2FeatureAndWordAndFRQ"))
    vec2FeatureAndWordAndFRQ.toList.sortBy(_._2._3).reverse.foreach(x => writer.println(x._1 + ":" + x._2._1 + ":" + x._2._2 + ":" + x._2._3))
    writer.close()
  }

  def reduceWordRepetition(str: String): (String, Int) = {
    val tokens = str.split(delimiterToken)
    var out = ""
    var prev = tokens(0)
    var counter = 1
    var denied = 0
    for (word <- tokens) {
      if (word.equals(prev))
        counter += 1
      else
        counter = 1
      if (counter <= MAX_NUMBER_OF_QUERY_REPETITION)
        out += (word + delimiterToken)
      else
        denied += 1
      prev = word

    }
    (out.dropRight(0), denied)
  }

  def reduceVectorRepetition(vecs: Seq[String]): String = {
    var out = ""
    var prev = vecs(0)
    var counter = 0
    var denied = 0
    for (word <- vecs) {
      if (word.equals(prev))
        counter += 1
      else
        counter = 1
      if (counter <= MAX_NUMBER_OF_QUERY_REPETITION)
        out += (word + delimiterVector)
      else
        denied += 1
      prev = word
    }
    println(denied)
    out = out.dropRight(1)
    out
  }

  override def readConfiguration(args: Array[String]): Unit = {
    parentDir = "/home/hamid/QAL/QP/skyServer/"
    pathToQueryLog = parentDir + "workload/log6"
    pathToTableParquet = parentDir + "data_parquet/"
    resultPath = "/home/hamid/QAL/DensityCluster/" + "processVec_" + tag
    resultPathTest = "/home/hamid/QAL/DensityCluster/" + "processVec_" + tagTest
    resultWordPath = "/home/hamid/QAL/DensityCluster/" + "processWord_" + tag
    resultInfoPath = "/home/hamid/QAL/DensityCluster/" + "Info"
    chartPath = "/home/hamid/QAL/DensityCluster/" + "chart_" + tag + ".png"
    pathToQueryLogTest = parentDir + "workload/log6"
  }
}
