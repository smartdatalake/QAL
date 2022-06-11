package mains

import definition.Paths._
import mains.density_cluster.sparkSession
import org.apache.spark.sql.functions.col
import java.io.{File, PrintWriter}

import scala.collection.{Seq, mutable}
import sparkSession.implicits._

object workloadToVector extends QueryEngine_Abs("WorkloadToVector") {
  override def ReadNextQueries(query: String, ip: String, epoch: Long, queryIndex: Int): Seq[String] = null


  val tag = (MAX_NUMBER_OF_QUERY_REPETITION + "maxNumberOfRepetition_" + ACCESSED_COL_MIN_FREQUENCY + "featureMinFRQ_" + YEAR_FROM + "fromYear")
  var resultPath = ""
  var resultWordPath = ""
  var resultInfoPath = ""
  var chartPath = ""


  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  def main(args: Array[String]): Unit = {
    readConfiguration(args)
    loadTables(sparkSession)
    val queryLog = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "false")
      .option("inferSchema", "true").option("delimiter", ";").option("nullValue", "null").schema(logSchema).load(pathToQueryLog)
    val tuples = queryLog.filter("statement is not null")
      .filter(row => row.getAs[String]("statement").trim.length > 0 && row.getAs[Int]("yy") >= YEAR_FROM)
      .sort(col("yy").asc, col("mm").asc, col("dd").asc, col("hh").asc, col("mi").asc, col("ss").asc, col("seq").asc)
      .select("yy", "mm", "dd", "hh", "mi", "ss", "statement")
      .map(_.getAs[String](6))
      .collect()

    val (vectors, accessedColToVectorIndex, groupByKeyToVectorIndex, joinKeyToVectorIndex, tableToVectorIndex
    , accessedColFRQ, groupByFRQ, joinKeyFRQ, tableFRQ, vec2FeatureAndFRQ) = queryToVector(tuples, sparkSession)
    var counter = 0
    var writer = new PrintWriter(new File(resultPath))
    var pre = vectors(0)
    var emptyVectorCNT = 0
    var repetitiveCNT = 0
    for (vector <- vectors) if (vector.contains("1")) {
      if (vector.equals(pre))
        counter += 1
      else
        counter = 1
      if (counter <= MAX_NUMBER_OF_QUERY_REPETITION)
        writer.println(vector)
      else
        repetitiveCNT += 1
      pre = vector
    }
    else
      emptyVectorCNT += 1
    writer.close()
    vec2word(vec2FeatureAndFRQ)
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
    vec2FeatureAndFRQ.toList.foreach(x => writer.println(x._1 + delimiterHashMap + x._2))
    writer.close()
    println("done")
  }

  def vec2word(vec2Feature: mutable.HashMap[String, (String, Long)]) = {
    val vec2FeatureAndWordAndFRQ = new mutable.HashMap[String, (String, String, Long)]()

    val writer = new PrintWriter(new File(resultWordPath + "_vec2FeatureAndWordAndFRQ"))
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
    pathToQueryLog = parentDir + "workload/skyServerExecutableCleaned_2008From"
    pathToTableParquet = parentDir + "data_parquet/"
    resultPath = "/home/hamid/QAL/DensityCluster/" + "queryVec_" + tag
    resultWordPath = "/home/hamid/QAL/DensityCluster/" + "queryWord_" + tag
    resultInfoPath = "/home/hamid/QAL/DensityCluster/" + "Info"
  }
}
