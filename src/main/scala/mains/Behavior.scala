package mains

import java.io.{File, PrintWriter}
import java.util.Date

import definition.Paths._
import mains.density_cluster.sparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions.col
import sparkSession.implicits._

import scala.collection.mutable.ListBuffer
import scala.collection.{Seq, mutable}

object Behavior extends QueryEngine_Abs("Behavior") {

  override def ReadNextQueries(query: String, ip: String, epoch: Long, queryIndex: Int): Seq[String] = null


  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  def main(args: Array[String]): Unit = {

    val begin = new Date(2008, 0, 1, 0, 0, 0).getTime / 1000
    val end = new Date(2021, 0, 1, 0, 0, 0).getTime / 1000
    readConfiguration(args)
    loadTables(sparkSession)
    //val behaviror= sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "false")
    //   .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").schema(processRecordSchema).load("/home/hamid/behavior")
    // behaviror.groupBy("p").count().show(100000)
    val queryLog = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "false")
      .option("inferSchema", "true").option("delimiter", ";").option("nullValue", "null").schema(logSchema).load(pathToQueryLog)
    println(queryLog.count())
    val tuples = queryLog.filter("statement is not null and clientIP is not null")
      .filter(row => row.getAs[String]("statement").trim.length > 0 && row.getAs[String]("clientIP").trim.length > 0)
      .sort(col("clientIP").asc, col("yy").asc, col("mm").asc, col("dd").asc, col("hh").asc, col("mi").asc, col("ss").asc, col("seq").asc)
      .select("clientIP", "yy", "mm", "dd", "hh", "mi", "ss", "statement").map(x => (x.getAs[String](0), new Date(x.getAs[Int](1), x.getAs[Int](2), x.getAs[Int](3), x.getAs[Int](4), x.getAs[Int](5), x.getAs[Int](6)).getTime / 1000, x.getAs[String](7), x.getAs[Int](1) + "," + x.getAs[Int](2) + "," + x.getAs[Int](3)
      + "," + x.getAs[Int](4) + "," + x.getAs[Int](5) + "," + x.getAs[Int](6))).collect().map(x => (x._1, x._2, x._3, sparkSession.sqlContext.sql(x._3).queryExecution.analyzed, x._4))

    val processesTrain = new ListBuffer[ListBuffer[(String, Long, String, LogicalPlan,String)]]
    if (tuples.size > 0 && tuples.size > 0) {
      var temp = new ListBuffer[(String, Long, String, LogicalPlan,String)]
      temp.+=(tuples(0))
      for (i <- 1 to tuples.size - 1)
        if (tuples(i)._1.equals(tuples(i - 1)._1) && 0 <= tuples(i)._2 - tuples(i - 1)._2 && tuples(i)._2 - tuples(i - 1)._2 <= gap)
          temp.+=(tuples(i))
        else {
          if (temp.size >= minProcessLength)
            processesTrain.+=(temp)
          temp = new ListBuffer[(String, Long, String, LogicalPlan,String)]
          temp.+=(tuples(i))
        }
      if (temp.size > minProcessLength)
        processesTrain.+=(temp)
    }
    val (t, aggregateColFRQ, groupByFRQ, joinKeyFRQ, tableFRQ, tableName, vectorToQ) = behavior2(processesTrain.flatten.sortBy(_._2), sparkSession)


    //val (processesVec) = behavior(processesTrain, sparkSession)
    t.groupBy(x=>x._3.split(",")(0)).foreach(x=>{
      val writer = new PrintWriter(new File("/home/hamid/behavior2"+x._1))
      x._2.foreach(t => writer.println(t._4 + "," + t._3))
      writer.close()
    })

    val writer = new PrintWriter(new File("/home/hamid/behavior2" + "_featureFRQ"))
    (aggregateColFRQ.toList ++ groupByFRQ.toList ++ joinKeyFRQ.toList ++ tableFRQ.toList).sortBy(_._2).foreach(x => writer.println(x._1 + delimiterHashMap + x._2))
    writer.close()

  }


  def vec2word(vec2Feature: mutable.HashMap[String, (String, Long)]) = {
    val vec2FeatureAndWordAndFRQ = new mutable.HashMap[String, (String, String, Long)]()
    var index = 1
    sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "false")
      .option("inferSchema", "true").option("delimiter", "%").option("nullValue", "null").load("resultPath")
      .flatMap(x => x.getAs[String](0).split(";")).distinct().collect().toList.foreach(vec => {
      vec2FeatureAndWordAndFRQ.put(vec, (vec2Feature(vec)._1, "Q" + index, vec2Feature(vec)._2))
      index += 1
    })
    var writer = new PrintWriter(new File("resultWordPath"))
    sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "false")
      .option("inferSchema", "true").option("delimiter", "%").option("nullValue", "null").load("resultPath")
      .map(row => row.getAs[String](0).split(";").map(vec2FeatureAndWordAndFRQ.get(_).get._2).mkString(" ")).collect().foreach(process => writer.println(process))
    writer.close()

    writer = new PrintWriter(new File("resultWordPath" + "_vec2FeatureAndWordAndFRQ"))
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
    out = out.dropRight(1)
    out
  }

  override def readConfiguration(args: Array[String]): Unit

  = {
    parentDir = "/home/hamid/QAL/QP/skyServer/"
    pathToQueryLog = parentDir + "workload/skyServerExecutableCleaned_From20080101_To20200101"
    //pathToQueryLog = parentDir + "workload/small"
    pathToTableParquet = parentDir + "data_parquet/"

    //pathToQueryLogTest = parentDir + "workload/skyServerExecutableSorted_2020From"
  }


}

