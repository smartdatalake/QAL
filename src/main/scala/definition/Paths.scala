package definition

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import operators.physical.SampleExec
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.LeafNode
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ComplexTypeMergingExpression, NamedExpression, UnaryExpression, _}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project, Sort, SubqueryAlias}
import org.apache.spark.sql.execution.{LeafExecNode, LogicalRDD, RDDScanExec, SparkPlan}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import sketch.Sketch

import java.util
import scala.collection.mutable.{HashMap, HashSet, ListBuffer}
import scala.collection.{Seq, mutable}
import scala.io.Source

object Paths {
  //val parentDir = "/home/hamid/TASTER/"
  var parentDir = "/home/hamid/QAL/"
  var pathToTableCSV = parentDir + "data_csv/"
  var pathToSketches = parentDir + "materializedSketches/"
  var pathToQueryLog = parentDir + "queryLog"
  var pathToTableParquet = parentDir + "data_parquet/"
  var pathToSaveSynopses = parentDir + "materializedSynopsis/"
  var pathToCIStats = parentDir + "CIstats/"
  val tableName: mutable.HashMap[String, String] = new mutable.HashMap()
  val seed = 5427500315423L
  var REST = false
  val sketchesMaterialized = new mutable.HashMap[String, Sketch]()
  var counterForQueryRow = 0
  var outputOfQuery = ""
  val delimiterSynopsesColumnName = "@"
  val delimiterSynopsisFileNameAtt = ";"
  val delimiterParquetColumn = ","
  val startSamplingRate = 5
  val stopSamplingRate = 50
  val samplingStep = 5
  var LRUorWindowBased: Boolean = false
  var lastUsedCounter: Long = 0
  val testFraction = Array(0.1)
  val testWindowSize = Array(50)
  var maxSpace = 1000
  var windowSize = 5
  var fractionInitialize = 1.0
  val start = 0
  val testSize = 35000
  var numberOfSynopsesReuse = 0
  var numberOfGeneratedSynopses = 0
  val minNumOfOcc = 15
  var counterNumberOfRowGenerated = 0
  var timeForUpdateWarehouse: Long = 0
  var timeForSampleConstruction: Long = 0
  var timeTotal: Long = 0
  var timeForSubQueryExecution: Long = 0
  var timeForTokenizing: Long = 0
  val costOfFilter: Long = 1
  val costOfProject: Long = 1
  val costOfScan: Long = 1
  val costOfJoin: Long = 1
  val filterRatio: Double = 0.9
  val costOfUniformSample: Long = 1
  val costOfUniformWithoutCISample: Long = 1
  val costOfUniversalSample: Long = 1
  val costOfDistinctSample: Long = 1
  val costOfScale: Long = 1
  val HashAggregate: Long = 1
  val SortAggregate: Long = 1
  val lastUsedOfParquetSample: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()
  val tableToCount: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()
  val warehouseParquetNameToSize = new mutable.HashMap[String, Long]()
  val ParquetNameToSynopses = mutable.HashMap[String, String]()
  val SynopsesToParquetName = mutable.HashMap[String, String]()
  val parquetNameToHeader = new mutable.HashMap[String, String]()
  var JDBCRowLimiter: Long = 100000000
  var mapRDDScanRowCNT: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()

  val ACCESSED_COL_MIN_FREQUENCY = 100
  val GROUPBY_COL_MIN_FREQUENCY = 100
  val JOIN_COL_MIN_FREQUENCY = 100
  val TABLE_MIN_FREQUENCY = 1
  val MAX_NUMBER_OF_QUERY_REPETITION = 100000

  var Proteus_URL = ""
  var Proteus_username = ""
  var Proteus_pass = ""
  var REST_PORT = 4545
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

  def getSizeOfAtt(in: Seq[Attribute]) = in.map(x => x.dataType.defaultSize).reduce(_ + _)

  def getHeaderOfOutput(output: Seq[Attribute]): String =
    output.map(o => tableName.get(o.toString().toLowerCase).get + "." + o.name.split("#")(0).toLowerCase).mkString(delimiterParquetColumn)

  def getAccessedColsOfExpressions(exps: Seq[Expression]): Seq[String] = {
    exps.flatMap(exp => getAccessedColsOfExpression(exp))
  }

  def getAttRefOfExps(exps: Seq[NamedExpression]): Seq[AttributeReference] =
    exps.map(_.find(_.isInstanceOf[AttributeReference]).map(_.asInstanceOf[AttributeReference]).get)

  def getAttOfExpression(exps: Seq[NamedExpression]): Seq[AttributeReference] =
    exps.map(o => {
      o.find(_.isInstanceOf[AttributeReference]).map(_.asInstanceOf[AttributeReference]).get
    })

  def getTableColumnsName(output: Seq[Attribute]): Seq[String] =
    output.map(o => tableName.get(o.toString().toLowerCase).get.split("\\.")(0).dropRight(2) + "." + o.name.split("#")(0).toLowerCase)

  def getAttNameOfJoinKey(joinKeys: Seq[AttributeReference]): Seq[String] =
    joinKeys.map(o => tableName.get(o.toString().toLowerCase).get + "." + o.name.split("#")(0).toLowerCase)

  def getAttNameOfAtt(att: Attribute): String = if (tableName.get(att.toString().toLowerCase).isDefined)
    tableName.get(att.toString().toLowerCase).get + "." + att.name.split("#")(0).toLowerCase
  else
    "userDefinedColumn"

  def getEqualToFromExpression(exp: Expression): Seq[org.apache.spark.sql.catalyst.expressions.EqualTo] = exp match {
    case a: UnaryExpression =>
      Seq()
    case b@org.apache.spark.sql.catalyst.expressions.EqualTo(left, right) =>
      if (left.isInstanceOf[AttributeReference] && right.isInstanceOf[AttributeReference])
        Seq(b)
      else
        Seq()
    case b: BinaryExpression =>
      getEqualToFromExpression(b.left) ++ getEqualToFromExpression(b.right)
    case _ =>
      Seq()
  }

  def checkJoin(lp: LogicalPlan): Boolean = lp match {
    case j: Join =>
      true
    case l: LogicalRDD =>
      false
    case n =>
      n.children.foreach(child => if (checkJoin(child)) return true)
      false
  }

  def updateAttributeName(lp: LogicalPlan): Unit = lp match {
    case SubqueryAlias(identifier, child@LogicalRDD(output, rdd, outputPartitioning, outputOrdering, isStreaming)) =>
      val att = output.toList
      for (i <- 0 to output.size - 1)
        tableName.put(att(i).toAttribute.toString().toLowerCase, (identifier.identifier).toLowerCase)
    case a =>
      a.children.foreach(x => updateAttributeName(x))
  }

  def updateAttributeName(lp: LogicalPlan, tableName: mutable.HashMap[String, String]): Unit = lp match {
    case SubqueryAlias(identifier, child@LogicalRDD(output, rdd, outputPartitioning, outputOrdering, isStreaming)) =>
      val att = output.toList
      for (i <- 0 to output.size - 1)
        tableName.put(att(i).toAttribute.toString().toLowerCase, (identifier.identifier).toLowerCase)
    case a =>
      a.children.foreach(x => updateAttributeName(x))
  }

  def extractAccessedColumn(lp: LogicalPlan, accessedCol: mutable.HashSet[String]): Unit = lp match {
    case a@Aggregate(groupingExpressions, aggregateExpressions, child) =>
      getAccessedColsOfExpressions(aggregateExpressions).foreach(x => accessedCol.add(x))
      getAccessedColsOfExpressions(groupingExpressions).foreach(x => accessedCol.add(x))
      child.foreach(c => extractAccessedColumn(c, accessedCol))
    case j@Join(left, right, joinType, condition) =>
      getAccessedColsOfExpression(condition.getOrElse(null)).foreach(x => accessedCol.add(x))
      extractAccessedColumn(left, accessedCol)
      extractAccessedColumn(right, accessedCol)
    case f@Filter(condition, child) =>
      getAccessedColsOfExpression(condition).foreach(x => accessedCol.add(x))
      extractAccessedColumn(child, accessedCol)
    case p@Project(projectList, child) =>
      getAccessedColsOfExpressions(projectList).foreach(x => accessedCol.add(x))
      extractAccessedColumn(child, accessedCol)
    case s@Sort(order: Seq[SortOrder], global, child) =>
      order.foreach(o => getAccessedColsOfExpressions(Seq(o.child)).foreach(o => accessedCol.add(o)))
      extractAccessedColumn(child, accessedCol)
    case s@SubqueryAlias(name, child) =>
      extractAccessedColumn(child, accessedCol)
    case l: LogicalRDD =>
      Unit
    case w@Window(windowExpressions, partitionSpec, orderSpec, child) =>
      orderSpec.foreach(o => getAccessedColsOfExpressions(Seq(o.child)).foreach(o => accessedCol.add(o)))
      getAccessedColsOfExpressions(windowExpressions).foreach(x => accessedCol.add(x))
      getAccessedColsOfExpressions(partitionSpec).foreach(x => accessedCol.add(x))
      child.foreach(c => extractAccessedColumn(c, accessedCol))
    case o: OneRowRelation =>
      Unit
    case d@Distinct(child) =>
      extractAccessedColumn(child, accessedCol)
    case a =>
      a.children.foreach(c => extractAccessedColumn(c, accessedCol))
  }

  def getAccessedColsOfExpression(exp: Expression): Seq[String] = exp match {
    case a@And(left, right) =>
      getAccessedColsOfExpression(left) ++ getAccessedColsOfExpression(right)
    case o@Or(left, right) =>
      getAccessedColsOfExpression(left) ++ getAccessedColsOfExpression(right)
    case b@BinaryOperator(left, right) =>
      (getAccessedColsOfExpression(left) ++ getAccessedColsOfExpression(right))
    case a: AttributeReference =>
      Seq(getAttNameOfAtt(a))
    case c@Cast(child, dataType, timeZoneId) => if (child.find(_.isInstanceOf[AttributeReference]).isDefined)
      getAccessedColsOfExpression(child)
    else
      Seq()
    case null =>
      Seq()
    case l: Literal =>
      Seq()
    case u: UnaryExpression => if (u.child.find(_.isInstanceOf[AttributeReference]).isDefined)
      getAccessedColsOfExpression(u.child)
    else
      Seq()
    case p: Predicate =>
      p.children.flatMap(x => if (x.find(_.isInstanceOf[AttributeReference]).isDefined)
        getAccessedColsOfExpression(x)
      else
        Seq())
    case a@AggregateExpression(aggregateFunction, mode, isDistinct, resultId) =>
      aggregateFunction.children.flatMap(x => getAccessedColsOfExpression(x))
    case c: ComplexTypeMergingExpression =>
      c.children.flatMap(x => getAccessedColsOfExpression(x))
    case a@Alias(child: Expression, name: String) =>
      getAccessedColsOfExpression(child)
    case e: Expression =>
      e.children.flatMap(x => getAccessedColsOfExpression(x))
    case a =>
      throw new Exception("Unknown Operator")
  }

  //todo fix joins
  def getJoinConditions(lp: LogicalPlan): Seq[String] = lp match {
    case j@Filter(condition, child) =>
      getJoinConditions(condition) ++ getJoinConditions(child)
    case j@Join(left, right, joinType, condition) if condition.isDefined =>
      getJoinConditions(condition.get) ++ getJoinConditions(left) ++ getJoinConditions(right)
    case l: LeafNode =>
      Seq()
    case a =>
      a.children.flatMap(x => getJoinConditions(x))
  }

  //todo fix joins u-g=r is ny join. het joins from logical or optimized
  def getJoinConditions(exp: Expression): Seq[String] = exp match {
    case And(left, right) =>
      getJoinConditions(left) ++ getJoinConditions(right)
    case Or(left, right) =>
      getJoinConditions(left) ++ getJoinConditions(right)
    case e@EqualTo(left, right) =>
      if (e.left.find(_.isInstanceOf[AttributeReference]).isDefined && e.right.find(_.isInstanceOf[AttributeReference]).isDefined
        && getAccessedColsOfExpression(e.left).size == 1 && getAccessedColsOfExpression(e.right).size == 1)
        Seq(Seq(getAccessedColsOfExpression(e.left)(0), getAccessedColsOfExpression(e.right)(0)).sortBy(_.toString).mkString("="))
      else
        Seq()
    //   case e: BinaryComparison =>
    //    if (e.left.find(_.isInstanceOf[AttributeReference]).isDefined && e.right.find(_.isInstanceOf[AttributeReference]).isDefined)
    //      Seq(getAccessedColsOfExpression(e.left).mkString(",") + "=" + getAccessedColsOfExpression(e.right).mkString(","))
    //    else
    //     getJoinConditions(e.left) ++ getJoinConditions(e.right)
    case BinaryOperator(left, right) =>
      getJoinConditions(left) ++ getJoinConditions(right)
    case e: Expression =>
      e.children.flatMap(x => getJoinConditions(x))
  }

  def getGroupByKeys(lp: LogicalPlan): Seq[String] = lp match {
    case Aggregate(groupingExpressions, aggregateExpressions, child) =>
      getAccessedColsOfExpressions(groupingExpressions) ++ getGroupByKeys(child)
    case l: LeafNode =>
      Seq()
    case a =>
      a.children.flatMap(x => getGroupByKeys(x))
  }

  def readSDLConfiguration() = {
    val json = Source.fromFile("QALconf.txt")
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val parsedJson = mapper.readValue[Map[String, String]](json.reader())
    fractionInitialize = parsedJson.getOrElse("fractionInitialize", "").toDouble
    maxSpace = parsedJson.getOrElse("maxSpace", "").toInt
    windowSize = parsedJson.getOrElse("windowSize", "").toInt
    REST = parsedJson.getOrElse("REST", "").toBoolean
    JDBCRowLimiter = parsedJson.getOrElse("JDBCRowLimiter", "").toInt
    parentDir = parsedJson.getOrElse("parentDir", "")
    Paths.Proteus_URL = parsedJson.getOrElse("ProteusJDBC_URL", "")
    Paths.Proteus_username = parsedJson.getOrElse("ProteusUsername", "")
    Paths.Proteus_pass = parsedJson.getOrElse("ProteusPassword", "")
    Paths.REST_PORT = parsedJson.getOrElse("port", "").toInt
    pathToTableCSV = parentDir //+ "data_csv/"
    pathToSketches = parentDir //+ "materializedSketches/"
    pathToQueryLog = parentDir //+ "queryLog"
    pathToTableParquet = parentDir //+ "data_parquet/"
    pathToSaveSynopses = parentDir // + "materializedSynopsis/"
    pathToCIStats = parentDir //+ "CIstats/"
    /*  pathToTableCSV.toFile.createIfNotExists()
      pathToSketches.toFile.createIfNotExists()
      pathToQueryLog.toFile.createIfNotExists()
      pathToTableParquet.toFile.createIfNotExists()
      pathToSaveSynopses.toFile.createIfNotExists()
      pathToCIStats.toFile.createIfNotExists()*/
  }

  def getScalingFactor(pp: SparkPlan): Double = pp match {
    case s: SampleExec =>
      (1 / s.fraction) * getScalingFactor(s.child)
    case l@RDDScanExec(output, rdd, name, outputPartitioning, outputOrdering) =>
      if (name.contains("sample"))
        1 / ParquetNameToSynopses.get(name).get.split(delimiterSynopsisFileNameAtt)(4).toDouble
      else
        1.0
    case a =>
      a.children.map(getScalingFactor).reduce(_ * _)
  }

  def saveExecutableQuery(queryLog: DataFrame, sparkSession: SparkSession, pathToSave: String): Unit = {
    queryLog.filter("statement is not null").filter(row => row.getAs[String]("statement").trim.length > 0)
      .filter(x => {
        try {
          if (sparkSession.sqlContext.sql(x.getAs[String]("statement")).queryExecution.analyzed != null) true else false
        }
        catch {
          case _ => false
        }
      }).write.format("com.databricks.spark.csv")
      .option("header", "false").option("delimiter", ";").option("nullValue", "null")
      .save(pathToSave)
  }

  def queryToVector(queriesStatement: Seq[String], sparkSession: SparkSession): (ListBuffer[String], mutable.HashMap[String, Int]
    , mutable.HashMap[String, Int], mutable.HashMap[String, Int], mutable.HashMap[String, Int], mutable.HashMap[String, Int]
    , mutable.HashMap[String, Int]) = {

    val accessedColFRQ = new HashMap[String, Int]()
    val groupByFRQ = new HashMap[String, Int]()
    val joinKeyFRQ = new HashMap[String, Int]()
    val tableFRQ = new HashMap[String, Int]()
    val accessedColToVectorIndex = new HashMap[String, Int]()
    val groupByKeyToVectorIndex = new HashMap[String, Int]()
    val joinKeyToVectorIndex = new HashMap[String, Int]()
    val tableToVectorIndex = new HashMap[String, Int]()
    val sequenceOfQueryEncoding = new ListBuffer[QueryEncoding]()
    val tableName: HashMap[String, String] = new HashMap()
    for (query <- queriesStatement) {
      val lp = sparkSession.sqlContext.sql(query).queryExecution.analyzed
      tableName.clear()
      updateAttributeName(lp, tableName)
      val accessedColsSet = new HashSet[String]()
      extractAccessedColumn(lp, accessedColsSet)
      val accessedCols = accessedColsSet.toSeq.filter(!_.contains("userDefinedColumn"))
      val joinKeys = Paths.getJoinConditions(lp).filter(!_.contains("userDefinedColumn"))
      val groupByKeys = getGroupByKeys(lp).filter(!_.contains("userDefinedColumn"))
      val tables = getTables(lp)
      for (col <- accessedCols)
        accessedColFRQ.put(col, accessedColFRQ.getOrElse(col, 0) + 1)
      for (key <- groupByKeys)
        groupByFRQ.put(key, groupByFRQ.getOrElse(key, 0) + 1)
      for (key <- joinKeys)
        joinKeyFRQ.put(key, joinKeyFRQ.getOrElse(key, 0) + 1)
      for (table <- tables)
        tableFRQ.put(table, tableFRQ.getOrElse(table, 0) + 1)
      sequenceOfQueryEncoding.+=(new QueryEncoding(accessedCols, groupByKeys, joinKeys, tables, query, 0))
    }
    var vectorIndex = 0
    for (col <- accessedColFRQ.filter(_._2 >= ACCESSED_COL_MIN_FREQUENCY).map(_._1).toList.sortBy(x => x))
      if (!accessedColToVectorIndex.get(col).isDefined) {
        accessedColToVectorIndex.put(col, vectorIndex)
        vectorIndex += 1
      }
    for (col <- groupByFRQ.filter(_._2 >= GROUPBY_COL_MIN_FREQUENCY).map(_._1).toList)
      if (!groupByKeyToVectorIndex.get(col).isDefined) {
        groupByKeyToVectorIndex.put(col, vectorIndex)
        vectorIndex += 1
      }
    for (col <- joinKeyFRQ.filter(_._2 >= JOIN_COL_MIN_FREQUENCY).map(_._1).toList)
      if (!joinKeyToVectorIndex.get(col).isDefined) {
        joinKeyToVectorIndex.put(col, vectorIndex)
        vectorIndex += 1
      }
    for (table <- tableFRQ.filter(_._2 >= TABLE_MIN_FREQUENCY).map(_._1).toList)
      if (!tableToVectorIndex.get(table).isDefined) {
        joinKeyToVectorIndex.put(table, vectorIndex)
        vectorIndex += 1
      }
    val vectorSize = (vectorIndex)
    (sequenceOfQueryEncoding.map(queryEncoding => {
      val vector = new Array[Int](vectorSize)
      util.Arrays.fill(vector, 0)
      for (accCol <- queryEncoding.accessedCols) if (accessedColToVectorIndex.get(accCol).isDefined)
        vector(accessedColToVectorIndex.get(accCol).get) = 1
      for (groupCol <- queryEncoding.groupByKeys) if (groupByKeyToVectorIndex.get(groupCol).isDefined)
        vector(groupByKeyToVectorIndex.get(groupCol).get) = 1
      for (joinCol <- queryEncoding.joinKeys) if (joinKeyToVectorIndex.get(joinCol).isDefined)
        vector(joinKeyToVectorIndex.get(joinCol).get) = 1
      for (table <- queryEncoding.tables) if (tableToVectorIndex.get(table).isDefined)
        vector(tableToVectorIndex.get(table).get) = 1
      vector.mkString("")
    }), accessedColToVectorIndex, groupByKeyToVectorIndex, joinKeyToVectorIndex, accessedColFRQ, groupByFRQ, joinKeyFRQ)
  }

  def processToVector(processes: ListBuffer[ListBuffer[(String, Int, Int, Int, Int, Int, Int, Long, String)]]
                      , sparkSession: SparkSession): (ListBuffer[ListBuffer[String]], mutable.HashMap[String, Int], mutable.HashMap[String, Int], mutable.HashMap[String, Int], mutable.HashMap[String, Int], mutable.HashMap[String, Int], mutable.HashMap[String, Int], mutable.HashMap[String, Int], mutable.HashMap[String, Int], mutable.HashMap[String, (String, Long)]) = {

    val accessedColFRQ = new HashMap[String, Int]()
    val groupByFRQ = new HashMap[String, Int]()
    val joinKeyFRQ = new HashMap[String, Int]()
    val tableFRQ = new HashMap[String, Int]()
    val vec2FeatureAndFRQ = new HashMap[String, (String, Long)]()
    val accessedColToVectorIndex = new HashMap[String, Int]()
    val groupByKeyToVectorIndex = new HashMap[String, Int]()
    val joinKeyToVectorIndex = new HashMap[String, Int]()
    val tableToVectorIndex = new HashMap[String, Int]()
    val sequenceOfQueryEncoding = new ListBuffer[ListBuffer[QueryEncoding]]()
    val tableName: HashMap[String, String] = new HashMap()
    for (process <- processes) {
      val processTemp = new ListBuffer[QueryEncoding]()
      for (query <- process) {
        val lp = sparkSession.sqlContext.sql(query._9).queryExecution.analyzed
        tableName.clear()
        updateAttributeName(lp, tableName)
        val accessedColsSet = new HashSet[String]()
        extractAccessedColumn(lp, accessedColsSet)
        val accessedCols = accessedColsSet.toSeq.distinct.sortBy(_.toString).filter(!_.contains("userDefinedColumn"))
        val joinKeys = Paths.getJoinConditions(lp).distinct.sortBy(_.toString).filter(!_.contains("userDefinedColumn"))
        val groupByKeys = getGroupByKeys(lp).distinct.sortBy(_.toString).filter(!_.contains("userDefinedColumn"))
        val tables = getTables(lp).distinct.sortBy(_.toString)
        for (col <- accessedCols)
          accessedColFRQ.put(col, accessedColFRQ.getOrElse(col, 0) + 1)
        for (key <- groupByKeys)
          groupByFRQ.put(key, groupByFRQ.getOrElse(key, 0) + 1)
        for (key <- joinKeys)
          joinKeyFRQ.put(key, joinKeyFRQ.getOrElse(key, 0) + 1)
        for (table <- tables)
          tableFRQ.put(table, tableFRQ.getOrElse(table, 0) + 1)
        processTemp.+=(new QueryEncoding(accessedCols, groupByKeys, joinKeys, tables, query._9, query._8))
      }
      sequenceOfQueryEncoding.+=(processTemp)
    }
    var vectorIndex = 0
    for (col <- accessedColFRQ.filter(_._2 >= ACCESSED_COL_MIN_FREQUENCY).map(_._1).toList.sortBy(_.toString))
      if (!accessedColToVectorIndex.get(col).isDefined) {
        accessedColToVectorIndex.put(col, vectorIndex)
        vectorIndex += 1
      }
    for (col <- groupByFRQ.filter(_._2 >= GROUPBY_COL_MIN_FREQUENCY).map(_._1).toList.sortBy(_.toString))
      if (!groupByKeyToVectorIndex.get(col).isDefined) {
        groupByKeyToVectorIndex.put(col, vectorIndex)
        vectorIndex += 1
      }
    for (col <- joinKeyFRQ.filter(_._2 >= JOIN_COL_MIN_FREQUENCY).map(_._1).toList.sortBy(_.toString))
      if (!joinKeyToVectorIndex.get(col).isDefined) {
        joinKeyToVectorIndex.put(col, vectorIndex)
        vectorIndex += 1
      }
    for (table <- tableFRQ.filter(_._2 >= TABLE_MIN_FREQUENCY).map(_._1).toList.sortBy(_.toString))
      if (!tableToVectorIndex.get(table).isDefined) {
        tableToVectorIndex.put(table, vectorIndex)
        vectorIndex += 1
      }
    val vectorSize = (vectorIndex)

    /*(sequenceOfQueryEncoding.map(processes => processes.map(queryEncoding => {
      (queryEncoding.accessedCols.toList.sortBy(_.toString).mkString(",") + "@" + queryEncoding.groupByKeys.toList.sortBy(_.toString).mkString(",") + "@"
        + queryEncoding.joinKeys.toList.sortBy(_.toString).mkString(",") + "@" + queryEncoding.tables.toList.sortBy(_.toString).mkString(","))
    })), accessedColToVectorIndex, groupByKeyToVectorIndex, joinKeyToVectorIndex, tableToVectorIndex, accessedColFRQ, groupByFRQ, joinKeyFRQ, tableFRQ)
*/

    (sequenceOfQueryEncoding.map(processes => processes.map(queryEncoding => {
      val vector = new Array[Int](vectorSize)
      util.Arrays.fill(vector, 0)
      for (accCol <- queryEncoding.accessedCols) if (accessedColToVectorIndex.get(accCol).isDefined)
        vector(accessedColToVectorIndex.get(accCol).get) = 1
      for (groupCol <- queryEncoding.groupByKeys) if (groupByKeyToVectorIndex.get(groupCol).isDefined)
        vector(groupByKeyToVectorIndex.get(groupCol).get) = 1
      for (joinCol <- queryEncoding.joinKeys) if (joinKeyToVectorIndex.get(joinCol).isDefined)
        vector(joinKeyToVectorIndex.get(joinCol).get) = 1
      for (table <- queryEncoding.tables) if (tableToVectorIndex.get(table).isDefined)
        vector(tableToVectorIndex.get(table).get) = 1
      vec2FeatureAndFRQ.put(vector.mkString(""), (queryEncoding.accessedCols.toList.sortBy(_.toString).mkString(",") + "@"
        + queryEncoding.groupByKeys.toList.sortBy(_.toString).mkString(",") + "@" + queryEncoding.joinKeys.toList.sortBy(_.toString).mkString(",")
        + "@" + queryEncoding.tables.toList.sortBy(_.toString).mkString(","), vec2FeatureAndFRQ.getOrElse[(String, Long)](vector.mkString(""), ("", 0))._2 + 1))
      vector.mkString("")
    })), accessedColToVectorIndex, groupByKeyToVectorIndex, joinKeyToVectorIndex, tableToVectorIndex, accessedColFRQ, groupByFRQ, joinKeyFRQ, tableFRQ, vec2FeatureAndFRQ)

  }

  def getTables(lp: LogicalPlan): Seq[String] = lp match {
    case SubqueryAlias(identifier, child@LogicalRDD(output, rdd, outputPartitioning, outputOrdering, isStreaming)) =>
      Seq(identifier.identifier.toLowerCase)
    case t =>
      t.children.flatMap(getTables)
  }

}
