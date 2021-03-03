import definition.Paths._
import definition.Paths.{logSchema, processToVector}
import main.loadTables
import mainSDL.sparkSession
import org.apache.spark.sql.SparkSession
import scalax.chart.api.ChartPNGExporter
import scalax.chart.module.XYChartFactories.XYLineChart

import java.util.Date
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import sparkSession.implicits._

import java.io.{File, PrintWriter}

object density_cluster {

  val gap = 30 * 60
  val minProcessLength = 3
  val pathToQueryLog = "/home/hamid/TASTER/spark-data/skyServer/queryLogs/queryLogExecutable" //args(0)
  val dataDir = "/home/hamid/TASTER/spark-data/skyServer/sf1/data_parquet/"
  val dataPath = "/home/hamid/processFeature_" + gap + "Gap_" + minProcessLength + "processMinLength"
  val resultPath = parentDir + "processVec_" + gap + "Gap_" + minProcessLength + "processMinLength_" + ACCESSED_COL_MIN_FREQUENCY + "featureMinFRQ"
  val resultWordPath = parentDir + "processWord_" + gap + "Gap_" + minProcessLength + "processMinLength_" + ACCESSED_COL_MIN_FREQUENCY + "featureMinFRQ"
  val resultInfoPath = resultPath + "Info"
  val bench = "skyServer"

  def main(args: Array[String]): Unit = {
    SparkSession.setActiveSession(sparkSession)
    System.setProperty("geospark.global.charset", "utf8")
    sparkSession.sparkContext.setLogLevel("ERROR");
    sparkSession.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkSession.conf.set("spark.driver.maxResultSize", "8g")
    sparkSession.conf.set("spark.sql.crossJoin.enabled", true)
    loadTables(sparkSession, bench, dataDir)
    val processes = new ListBuffer[ListBuffer[(String, Int, Int, Int, Int, Int, Int, Long, String)]]
    val processFRQ = new mutable.HashMap[Int, Int]

    val queryLog = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ";").option("nullValue", "null").schema(logSchema).load(pathToQueryLog)
    val tuples = queryLog.filter("statement is not null and clientIP is not null")
      .filter(row => row.getAs[String]("statement").trim.length > 0 && row.getAs[String]("clientIP").trim.length > 0)
      .sort("clientIP", "yy", "mm", "dd", "hh", "mi", "ss", "seq")
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
          processes.+=(temp)
        temp = new ListBuffer[(String, Int, Int, Int, Int, Int, Int, Long, String)]
      }

    val (processesVec, accessedColToVectorIndex, groupByKeyToVectorIndex, joinKeyToVectorIndex, tableToVectorIndex
    , accessedColFRQ, groupByFRQ, joinKeyFRQ, tableFRQ, vec2FeatureAndFRQ) = processToVector(processes, sparkSession)
    var writer = new PrintWriter(new File(resultPath))
    processesVec.foreach(process => writer.println(process.mkString(";")))
    writer.close()
    writer = new PrintWriter(new File(resultPath + "_colIndex"))
    accessedColToVectorIndex.toList.sortBy(_._2).foreach(x => writer.println(x._1 + ":" + x._2))
    writer.close()
    writer = new PrintWriter(new File(resultPath + "_groupIndex"))
    groupByKeyToVectorIndex.toList.sortBy(_._2).foreach(x => writer.println(x._1 + ":" + x._2))
    writer.close()
    writer = new PrintWriter(new File(resultPath + "_joinIndex"))
    joinKeyToVectorIndex.toList.sortBy(_._2).foreach(x => writer.println(x._1 + ":" + x._2))
    writer.close()
    writer = new PrintWriter(new File(resultPath + "_tableIndex"))
    tableToVectorIndex.toList.sortBy(_._2).foreach(x => writer.println(x._1 + ":" + x._2))
    writer.close()
    writer = new PrintWriter(new File(resultPath + "_featureFRQ"))
    (accessedColFRQ.toList ++ groupByFRQ.toList ++ joinKeyFRQ.toList ++ tableFRQ.toList).sortBy(_._2).foreach(x => writer.println(x._1 + ":" + x._2))
    writer.close()
    writer = new PrintWriter(new File(resultPath + "_vec2Feature"))
    (vec2FeatureAndFRQ).toList.foreach(x => writer.println(x._1 + ":" + x._2))
    writer.close()
    vec2word(vec2FeatureAndFRQ)
    processes.map(x => (x(0)._1, x.map(tuple => tuple._9)))
    processes.foreach(x => processFRQ.put(x.size, processFRQ.getOrElse(x.size, 0) + 1))
    processFRQ.toList.sortBy(_._1).map(x => (x._1, x._2)).foreach(println)
    XYLineChart(processFRQ.toList.sortBy(_._1).map(x => (x._1, x._2)).take(100)).saveAsPNG("chartGap" + gap + "MinLength" + minProcessLength + ".png")
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
}
