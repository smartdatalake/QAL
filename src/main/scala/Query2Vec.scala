import main.loadTables
import org.apache.spark.sql.types._
import java.io.{File, PrintWriter}
import definition.{Paths, QueryEncoding}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import java.util
import org.apache.spark.sql.types.{StructField, StructType}
import definition.Paths._

object Query2Vec {
  var pathToQueryLog = ""
  val resultPath = "/home/hamid/vectorsIP_" + Paths.ACCESSED_COL_MIN_FREQUENCY + "_" + Paths.MAX_NUMBER_OF_QUERY_REPETITION
  val resultInfoPath = resultPath + "Info"
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
    val accessedColFRQ = new mutable.HashMap[String, Int]()
    val groupByFRQ = new mutable.HashMap[String, Int]()
    val joinKeyFRQ = new mutable.HashMap[String, Int]()
    var errorCNT = 0
    var totalCNT = 0
    pathToQueryLog = "/home/hamid/TASTER/spark-data/skyServer/queryLogs/pp" //args(0)
    val queryLog = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ";").option("nullValue", "null").schema(logSchema).load(pathToQueryLog)
    val queriesStatement = queryLog.filter("statement is not null").filter(row => row.getAs[String]("statement").trim.length > 0)
      .sort("clientIP", "yy", "mm", "dd", "hh", "mi", "ss", "seq").select("statement").map(_.getAs[String](0)).collect()
    for (q <- queriesStatement) {
      val query = q.replace("set showplan_all on go ", "").replace("select , count(1)", "select count(1)").replace("/* specobjid and qso only */ ", " ").replace("/* specobjid and qso only */ ", " ").replace(" font=\"\"", "").replace(" select count(*)", " ").replace("photoz2", "photoz").replace("as avg_dec, from", "as avg_dec from").replace("== SQL ==", "")
      totalCNT += 1
      if (totalCNT % 100000 == 1)
        println("total=" + totalCNT)
      if (errorCNT % 100000 == 1)
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
        val accessedCols = accessedColsSet.toSeq.filter(!_.contains("userDefinedColumn"))
        val joinKeys = Paths.getJoinConditions(lp).filter(!_.contains("userDefinedColumn"))
        val groupByKeys = getGroupByKeys(lp).filter(!_.contains("userDefinedColumn"))
        for (col <- accessedCols)
          accessedColFRQ.put(col, accessedColFRQ.getOrElse(col, 0) + 1)
        for (key <- groupByKeys)
          groupByFRQ.put(key, groupByFRQ.getOrElse(key, 0) + 1)
        for (key <- joinKeys)
          joinKeyFRQ.put(key, joinKeyFRQ.getOrElse(key, 0) + 1)
        sequenceOfQueryEncoding.+=(new QueryEncoding(accessedCols, groupByKeys, joinKeys, query, ""))
        //   println(new QueryEncoding(accessedCols, groupByKeys, joinKeys, query))
      }
    }
    var vectorIndex = 0
    for (col <- accessedColFRQ.filter(_._2 >= ACCESSED_COL_MIN_FREQUENCY).map(_._1).toList) {
      if (!accessedColToVectorIndex.get(col).isDefined) {
        accessedColToVectorIndex.put(col, vectorIndex)
        vectorIndex += 1
      }
    }
    for (col <- groupByFRQ.filter(_._2 >= GROUPBY_COL_MIN_FREQUENCY).map(_._1).toList) {
      if (!groupByKeyToVectorIndex.get(col).isDefined) {
        groupByKeyToVectorIndex.put(col, vectorIndex)
        vectorIndex += 1
      }
    }
    for (col <- joinKeyFRQ.filter(_._2 >= JOIN_COL_MIN_FREQUENCY).map(_._1).toList) {
      if (!joinKeyToVectorIndex.get(col).isDefined) {
        joinKeyToVectorIndex.put(col, vectorIndex)
        vectorIndex += 1
      }
    }
    val vectorSize = (vectorIndex)
    val vectors = sequenceOfQueryEncoding.map(queryEncoding => {
      val vector = new Array[Int](vectorSize)
      util.Arrays.fill(vector, 0)
      for (accCol <- queryEncoding.accessedCols) if (accessedColToVectorIndex.get(accCol).isDefined)
        vector(accessedColToVectorIndex.get(accCol).get) = 1
      for (groupCol <- queryEncoding.groupByKeys) if (groupByKeyToVectorIndex.get(groupCol).isDefined)
        vector(groupByKeyToVectorIndex.get(groupCol).get) = 1
      for (joinCol <- queryEncoding.joinKeys) if (joinKeyToVectorIndex.get(joinCol).isDefined)
        vector(joinKeyToVectorIndex.get(joinCol).get) = 1
      vector.mkString(",")
    })
    var counter = 0
    var writer = new PrintWriter(new File(resultPath))
    var pre = vectors(0)
    var emptyVectorCNT = 0
    var repetitiveCNT = 0
    for (vector <- vectors) if (vector.contains("1")) {
      if (vector.equals(pre))
        counter += 1
      else
        counter = 0
      if (counter < MAX_NUMBER_OF_QUERY_REPETITION)
        writer.println(vector)
      else
        repetitiveCNT += 1
      pre = vector
    }
    else
      emptyVectorCNT += 1
    writer.close()
    writer = new PrintWriter(new File(resultInfoPath))
    writer.println("total Queries: " + totalCNT)
    writer.println("error Queries: " + errorCNT)
    writer.println("empty Vectors: " + emptyVectorCNT)
    writer.println("repetitive Queries: " + repetitiveCNT)
    writer.println("Feature Count: " + vectorSize)
    writer.println("*** AccessedColIndex")
    for (x <- accessedColToVectorIndex.toList.sortBy(_._2))
      writer.println(x._1 + "#" + x._2)
    writer.println("*** GroupByIndex")
    for (x <- groupByKeyToVectorIndex.toList.sortBy(_._2))
      writer.println(x._1 + "#" + x._2)
    writer.println("*** JoinKeyIndex")
    for (x <- joinKeyToVectorIndex.toList.sortBy(_._2))
      writer.println(x._1 + "#" + x._2)
    writer.println("*** AccessedColFrequency")
    for (x <- accessedColFRQ.toList.sortBy(_._2).reverse)
      writer.println(x._1 + "#" + x._2)
    writer.println("*** GroupByFrequency")
    for (x <- groupByFRQ.toList.sortBy(_._2).reverse)
      writer.println(x._1 + "#" + x._2)
    writer.println("*** JoinKeyFrequency")
    for (x <- joinKeyFRQ.toList.sortBy(_._2).reverse)
      writer.println(x._1 + "#" + x._2)
    writer.close()
    println("total Queries: " + totalCNT)
    println("error Queries: " + errorCNT)
    println("empty Vectors: " + emptyVectorCNT)
    println("repetitive Queries: " + repetitiveCNT)
    println("Feature Count: " + vectorSize)
    println(accessedColFRQ)
    println(groupByFRQ)
    println(joinKeyFRQ)
    println(accessedColToVectorIndex)
    println(groupByKeyToVectorIndex)
    println(joinKeyToVectorIndex)
  }
}
