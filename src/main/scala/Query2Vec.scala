import main.{costOfPlan, getJoinConditions, loadTables, numberOfExecutedSubQuery, sparkSession}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import java.io.{BufferedReader, File, FileOutputStream, FileReader, FilenameFilter, PrintWriter}
import java.net.ServerSocket
import definition.{Paths, QueryEncoding, TableDefs}
import extraSQL.extraSQLOperators
import org.apache.spark.sql.catalyst.{AliasIdentifier, CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project, ReturnAnswer, Sort, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{CollapseCodegenStages, FilterExec, LogicalRDD, PlanLater, PlanSubqueries, ProjectExec, RDDScanExec, ReuseSubquery, SparkPlan}
import rules.logical.{ApproximateInjector, pushFilterUp}
import rules.physical.{ChangeSampleToScan, SampleTransformation, ScaleAggregateSampleExec}
import org.apache.spark.sql.catalyst.analysis._

import scala.collection.{Seq, mutable}
import scala.collection.mutable.ListBuffer
import scala.io.Source
import java.{lang, util}
import operators.physical.{DistinctSampleExec2, SampleExec, UniformSampleExec2, UniformSampleExec2WithoutCI, UniversalSampleExec2}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReuseExchange}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}

import scala.reflect.io.Directory
import definition.Paths._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.functions.col
import org.apache.spark.util.Utils

object Query2Vec {
  var pathToQueryLog = ""
  val logSchema = StructType(Array(
    StructField("yy", IntegerType, false),
    StructField("mm", IntegerType, false),
    StructField("dd", IntegerType, false),
    StructField("hh", IntegerType, false),
    StructField("mi", IntegerType, false),
    StructField("ss", IntegerType, false),
    StructField("seq", LongType, true),
    StructField("clientIP", StringType, true),
    StructField("rows", LongType, true),
    StructField("statement", StringType, true))
  )
  val sparkSession = SparkSession.builder
    .appName("Query2Vec")
    .master("local[*]")
    .getOrCreate();

  def main(args: Array[String]): Unit = {
/*
    val source = Source.fromFile("/home/hamid/vectors")
    var cnt=0
    var wr= new PrintWriter(new File("/home/hamid/vectorsCom"))
    var pre=source.getLines().next
    for (line <- source.getLines()) {
      if (line.equals(pre))
        cnt += 1
      else
        cnt = 0
      if (cnt < 3)
        wr.println(line)
      pre = line
    }
    source.close()
    wr.close()
    throw new Exception
*/

    SparkSession.setActiveSession(sparkSession)
    System.setProperty("geospark.global.charset", "utf8")
    sparkSession.sparkContext.setLogLevel("ERROR");
    sparkSession.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkSession.conf.set("spark.driver.maxResultSize", "8g")
    sparkSession.conf.set("spark.sql.codegen.wholeStage", false); // disable codegen
    sparkSession.conf.set("spark.sql.crossJoin.enabled", true)
    loadTables(sparkSession, "skyServer", "/home/hamid/TASTER/spark-data/skyServer/sf1/data_parquet/")

    import sparkSession.implicits._
    val accessedColToVectorIndex = new mutable.HashMap[String, Int]()
    val groupByKeyToVectorIndex = new mutable.HashMap[String, Int]()
    val joinKeyToVectorIndex = new mutable.HashMap[String, Int]()
    val sequenceOfQueryEncoding = new ListBuffer[QueryEncoding]()
    var temp=new mutable.HashMap[String,Int]()
    var errorCNT = 0
    var totalCNT = 0
    pathToQueryLog = "/home/hamid/TASTER/spark-data/skyServer/queryLogs/pp" //args(0)
    val queryLog = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ";").option("nullValue", "null").schema(logSchema).load(pathToQueryLog)
    val queriesStatement = queryLog.filter("statement is not null").filter(row => row.getAs[String]("statement").trim.length > 0)
      .sort("yy", "mm", "dd", "hh", "mi", "ss", "seq").select("statement").map(_.getAs[String](0)).collect()
    var indexAccessedCol: Int = 0
    var indexGroupByKey: Int = 0
    var indexJoinKey: Int = 0
    var correctQueryCNT = 0
    for (q <- queriesStatement) {
      val query = q.replace("set showplan_all on go ", "").replace("select , count(1)", "select count(1)").replace("/* specobjid and qso only */ ", " ").replace("/* specobjid and qso only */ ", " ").replace(" font=\"\"", "").replace(" select count(*)", " ").replace("photoz2", "photoz").replace("as avg_dec, from", "as avg_dec from").replace("== SQL ==", "")
      totalCNT += 1
      if (totalCNT % 10000 == 1)
        println("total=" + totalCNT)
      if (errorCNT % 10000 == 1)
        println("error=" + errorCNT)
      var lp: LogicalPlan = null
      try {
        lp = sparkSession.sqlContext.sql(query).queryExecution.analyzed
      }
      catch {
        case e: Exception =>
          errorCNT += 1
      }
      if (lp != null) {
        tableName.clear()
        updateAttributeName(lp)
        val accessedColsSet = new mutable.HashSet[String]()
        extractAccessedColumn(lp, accessedColsSet)
        val accessedCols = accessedColsSet.toSeq.filter(!_.equals("userDefinedColumn"))
        val joinKeys = Paths.getJoinConditions(lp).filter(!_.equals("userDefinedColumn"))
        val groupByKeys = getGroupByKeys(lp).filter(!_.equals("userDefinedColumn"))
        for (col <- accessedCols) {
          if (!accessedColToVectorIndex.get(col).isDefined) {
            accessedColToVectorIndex.put(col, indexAccessedCol)
            indexAccessedCol += 1
          }
          temp.put("col"+col,temp.getOrElse("col"+col,0)+1)
        }
        for (key <- groupByKeys) {
          if (!groupByKeyToVectorIndex.get(key).isDefined) {
            groupByKeyToVectorIndex.put(key, indexGroupByKey)
            indexGroupByKey += 1
          }
          temp.put("gro"+key,temp.getOrElse("gro"+key,0)+1)
        }
        for (key <- joinKeys) {
          if (!joinKeyToVectorIndex.get(key).isDefined) {
            joinKeyToVectorIndex.put(key, indexJoinKey)
            indexJoinKey += 1
          }
          temp.put("joi"+key,temp.getOrElse("joi"+key,0)+1)
        }
        sequenceOfQueryEncoding.+=(new QueryEncoding(accessedCols, groupByKeys, joinKeys, query))
      //   println(new QueryEncoding(accessedCols, groupByKeys, joinKeys, query))
      }
    }
    temp.toList.sortBy(_._2).foreach(println)
    val vectorSize = accessedColToVectorIndex.size + groupByKeyToVectorIndex.size + joinKeyToVectorIndex.size
    val vectors=sequenceOfQueryEncoding.map(queryEncoding => {
      val vector = new Array[Int](vectorSize)
      util.Arrays.fill(vector, 0)
      for (accCol <- queryEncoding.accessedCols)
        vector(accessedColToVectorIndex.get(accCol).get) = 1
      for (groupCol <- queryEncoding.groupByKeys)
        vector(accessedColToVectorIndex.size + groupByKeyToVectorIndex.get(groupCol).get) = 1
      for (joinCol <- queryEncoding.joinKeys)
        vector(accessedColToVectorIndex.size + groupByKeyToVectorIndex.size + joinKeyToVectorIndex.get(joinCol).get) = 1
      vector.mkString(",")
    })
    var writer= new PrintWriter(new File("/home/hamid/vectors"))
    for(s<-vectors)
      writer.println(s)
    writer.close()
    writer= new PrintWriter(new File("/home/hamid/vectorsInfo"))
    for(x<-accessedColToVectorIndex)
      writer.println(x._1+"#"+x._2)
    writer.println("***")
    for(x<-groupByKeyToVectorIndex)
      writer.println(x._1+"#"+x._2)
    writer.println("****")
    for(x<-joinKeyToVectorIndex)
      writer.println(x._1+"#"+x._2)
    writer.close()
    println(accessedColToVectorIndex)
    println(groupByKeyToVectorIndex)
    println(joinKeyToVectorIndex)
  }
}
