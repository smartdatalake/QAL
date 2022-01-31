package definition

import operators.physical.{SampleExec, SketchExec}
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.LeafNode
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ComplexTypeMergingExpression, UnaryExpression}
import org.apache.spark.sql.execution.{LogicalRDD, RDDScanExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import sketch.Sketch
import java.io.File
import scala.collection.mutable.{HashMap, HashSet}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project, Sort, SubqueryAlias}
import scala.collection.{Seq, mutable}
import scala.collection.mutable.ListBuffer
import java.util
import org.apache.spark.sql.catalyst.expressions._
import scala.reflect.io.Directory


object Paths {

  var fraction = 0.3
  val fractionStep = 0.1
  var costOfProject: Long = 1
  var costOfScan: Long = 1
  var costOfJoin: Long = 10
  var costOfShuffle: Long = 10
  var costOfUniformSample: Long = 10
  var costOfUniversalSample: Long = 10
  var costOfDistinctSample: Long = 25
  var parentDir = ""
  var pathToTableCSV = ""
  var pathToSketches = ""
  var pathToQueryLog = ""
  var pathToTableParquet = ""
  var pathToSaveSynopses = ""
  var pathToML_Info = ""

  // DELIMITERS
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  var outputOfQuery = ""
  val delimiterSynopsesColumnName = "@"
  val delimiterSynopsisFileNameAtt = ";"
  val delimiterParquetColumn = ","
  val delimiterSparkSQLNameAndID = "#"
  val delimiterToken = " "
  val delimiterProcess = "\n"
  val delimiterVector = ";"
  val delimiterHashMap = ":"

  // COST_MODEL
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  var LRUorWindowBased: Boolean = false
  var lastUsedCounter: Long = 0
  var maxSpace = 700

  val alpha = 0.25
  var windowSize = 10
  var futureWindowSize = 10
  var numberOfSynopsesReuse = 0
  var numberOfGeneratedSynopses = 0
  var numberOfRemovedSynopses = 0
  var numberOfExecutedSubQuery = 0
  var counterNumberOfGeneratedRow = 0
  val lastUsedOfParquetSample: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()
  val tableToCount: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()
  val warehouseParquetNameToSize = new mutable.HashMap[String, Long]()
  val synopsesSize = new mutable.HashMap[String, Long]()
  val warehouseParquetNameToRow = new mutable.HashMap[String, Long]()
  val ParquetNameToSynopses = mutable.HashMap[String, String]()
  val SynopsesToParquetName = mutable.HashMap[String, String]()
  val parquetNameToHeader = new mutable.HashMap[String, String]()
  val sampleToOutput = new mutable.HashMap[String, Seq[Attribute]]()
  var mapRDDScanRowCNT: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()
  val tableName: mutable.HashMap[String, String] = new mutable.HashMap()
  val sketchesMaterialized = new mutable.HashMap[String, Sketch]()

  // QUERY_LOG
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  var tag = ""
  val start = 0
  var testSize = 100
  var queryCNT = 0
  var counterForQueryRow = 0
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

  // SAMPLING
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  val seed = 5427500315423L
  val minNumOfOcc = 15

  // TIMERS
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  var timeForUpdateWarehouse: Long = 0
  var timeForSampleConstruction: Long = 0
  var timeTotal: Long = 0
  var timeForSubQueryExecution: Long = 0
  var timeForTokenizing: Long = 0

  // SDL
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


  // DENSITY_CLUSTER
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  val ACCESSED_COL_MIN_FREQUENCY = 0
  val GROUPBY_COL_MIN_FREQUENCY = 0
  val JOIN_COL_MIN_FREQUENCY = 0
  val TABLE_MIN_FREQUENCY = 0
  val MAX_NUMBER_OF_QUERY_REPETITION = 100
  val gap = 30 * 60
  val minProcessLength = 1
  val YEAR_FROM = 2007
  val reserveFeature = 5
  var featureCNT = 512


  def getAttRefFromExp(exp: Expression): Seq[AttributeReference] = exp match {
    case a: AttributeReference =>
      Seq(a)
    case l: LeafNode =>
      Seq()
    case _ =>
      exp.children.flatMap(getAttRefFromExp)
  }

  def getHeaderOfOutput(output: Seq[Expression]): String =
    output.flatMap(getAttRefFromExp).map(o => tableName.get(o.toString().toLowerCase).getOrElse("UserDefined") + "." + o.name.split("#")(0).toLowerCase).mkString(delimiterParquetColumn)

  def getAccessedColsOfExpressions(exps: Seq[Expression]): Seq[String] = {
    exps.flatMap(exp => getAccessedColsOfExpression(exp))
  }

  def getAttNameOfAtt(att: Attribute): String = if (att == null) "*"
  else if (tableName.get(att.toString().toLowerCase).isDefined)
    tableName.get(att.toString().toLowerCase).get + "." + att.name.split("#")(0).toLowerCase
  else
    "userDefinedColumn"

  def getAttNameOfAttWithoutTableCounter(att: Attribute): String = if (att == null) "*"
  else if (tableName.get(att.toString().toLowerCase).isDefined)
    tableName.get(att.toString().toLowerCase).get.dropRight(2) + "." + att.name.split("#")(0).toLowerCase
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

  def extractFilterColumns(lp: LogicalPlan):Seq[String]=lp match {
    case f@Filter(condition, child) =>
      extractFilterColumns(child) ++ getAccessedColsOfExpression(condition).toSeq
    case a =>
      a.children.flatMap(c => extractFilterColumns(c))
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
    case And(left, right) =>
      getAccessedColsOfExpression(left) ++ getAccessedColsOfExpression(right)
    case Or(left, right) =>
      getAccessedColsOfExpression(left) ++ getAccessedColsOfExpression(right)
    case BinaryOperator(left, right) =>
      (getAccessedColsOfExpression(left) ++ getAccessedColsOfExpression(right))
    case a: AttributeReference =>
      Seq(getAttNameOfAtt(a))
    case Cast(child, dataType, timeZoneId) => if (child.find(_.isInstanceOf[AttributeReference]).isDefined)
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
    case AggregateExpression(aggregateFunction, mode, isDistinct, resultId) =>
      aggregateFunction.children.flatMap(x => getAccessedColsOfExpression(x))
    case c: ComplexTypeMergingExpression =>
      c.children.flatMap(x => getAccessedColsOfExpression(x))
    case e: Expression =>
      e.children.flatMap(x => getAccessedColsOfExpression(x))
  }

  def getAggregateColumns(lp: LogicalPlan): Seq[String] = lp match {
    case a@Aggregate(groupingExpressions, aggregateExpressions, child) =>
      aggregateExpressions.filter(_.find(_.isInstanceOf[AggregateExpression]).isDefined).flatMap(x => {
        x.find(_.isInstanceOf[AggregateExpression]).get.asInstanceOf[AggregateExpression].aggregateFunction match {
          case s: org.apache.spark.sql.catalyst.expressions.aggregate.Sum =>
            getAccessedColsOfExpression(x).map(y => "sum(" + y + ")")
          case s: org.apache.spark.sql.catalyst.expressions.aggregate.Average =>
            getAccessedColsOfExpression(x).map(y => "avg(" + y + ")")
          case s: org.apache.spark.sql.catalyst.expressions.aggregate.Count =>
            getAccessedColsOfExpression(x).map(y => "count(" + y + ")")
          case _ =>
            Seq()
        }
      }).toSet.toSeq
    case _ =>
      lp.children.flatMap(getAggregateColumns)
  }


  //todo fix joins
  def getJoinConditions(lp: LogicalPlan): Seq[String] = lp match {
    case Filter(condition, child) =>
      getJoinConditions(condition) ++ getJoinConditions(child)
    case Join(left, right, joinType, condition) if condition.isDefined =>
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

  def loadTables(sparkSession: SparkSession): Unit = {
    (new File(pathToTableParquet)).listFiles.filter(_.getName.contains(".parquet")).foreach(table => {
      val view = sparkSession.read.parquet(table.getAbsolutePath);
      sparkSession.sqlContext.createDataFrame(view.rdd, view.schema).createOrReplaceTempView(table.getName.split("\\.")(0).toLowerCase);
    })
  }

  def saveExecutableQuery(queryLog: DataFrame, sparkSession: SparkSession, pathToSave: String): Unit = {
    queryLog.foreach(x => println(x.getAs[String]("statement")))
    queryLog.filter("statement is not null").filter(row => row.getAs[String]("statement").trim.length > 0)
      .filter(x => {
        println(x)
        try {
          if (sparkSession.sqlContext.sql(x.getAs[String]("statement")).queryExecution.analyzed != null)
            true
          else
            false
        }
        catch {
          case _: Throwable => false
        }
      })
      .filter(x => !x.getAs[String]("statement").contains("power(2,24)"))
      .filter(x => !x.getAs[String]("statement").contains("firstplate.plate = first.plate"))
      .write.format("com.databricks.spark.csv")
      .option("header", "false").option("delimiter", ";").option("nullValue", "null")
      .save(pathToSave)
  }

  def queryToVector(queriesStatement: Seq[String], sparkSession: SparkSession): (ListBuffer[String], mutable.HashMap[String, Int], mutable.HashMap[String, Int], mutable.HashMap[String, Int], mutable.HashMap[String, Int], mutable.HashMap[String, Int], mutable.HashMap[String, Int], mutable.HashMap[String, Int], mutable.HashMap[String, Int], mutable.HashMap[String, (String, Long)]) = {
    val aggregateColFRQ = new HashMap[String, Int]()
    // val accessedColFRQ = new HashMap[String, Int]()
    val groupByFRQ = new HashMap[String, Int]()
    val joinKeyFRQ = new HashMap[String, Int]()
    val tableFRQ = new HashMap[String, Int]()
    val vec2FeatureAndFRQ = new HashMap[String, (String, Long)]()
    val aggregateColToVectorIndex = new HashMap[String, Int]()
    //  val accessedColToVectorIndex = new HashMap[String, Int]()
    val groupByKeyToVectorIndex = new HashMap[String, Int]()
    val joinKeyToVectorIndex = new HashMap[String, Int]()
    val tableToVectorIndex = new HashMap[String, Int]()
    val sequenceOfQueryEncoding = new ListBuffer[QueryEncoding]()
    val tableName: HashMap[String, String] = new HashMap()
    for (query <- queriesStatement) {
      val lpp = sparkSession.sqlContext.sql(query).queryExecution.analyzed
      tableName.clear()
      updateAttributeName2(lpp, tableName)
      getAggSubQueries(lpp).map(lp => {
        val aggregateCol = Paths.getAggregateColumns(lp)
        val joinKeys = Paths.getJoinConditions(lp).distinct.sortBy(_.toString).filter(!_.contains("userDefinedColumn"))
        val groupByKeys = getGroupByKeys(lp).distinct.sortBy(_.toString).filter(!_.contains("userDefinedColumn"))
        val tables = getTables(lp).distinct.sortBy(_.toString)
        for (col <- aggregateCol)
          aggregateColFRQ.put(col, aggregateColFRQ.getOrElse(col, 0) + 1)
        //   for (col <- accessedCols)
        //     accessedColFRQ.put(col, accessedColFRQ.getOrElse(col, 0) + 1)
        for (key <- groupByKeys)
          groupByFRQ.put(key, groupByFRQ.getOrElse(key, 0) + 1)
        for (key <- joinKeys)
          joinKeyFRQ.put(key, joinKeyFRQ.getOrElse(key, 0) + 1)
        for (table <- tables)
          tableFRQ.put(table, tableFRQ.getOrElse(table, 0) + 1)
        sequenceOfQueryEncoding.+=(new QueryEncoding(aggregateCol, groupByKeys, joinKeys, tables, query, 0))
      })
    }
    var vectorIndex = 0
    for (col <- aggregateColFRQ.filter(_._2 >= ACCESSED_COL_MIN_FREQUENCY).map(_._1).toList.sortBy(_.toString))
      if (!aggregateColToVectorIndex.get(col).isDefined) {
        aggregateColToVectorIndex.put(col, vectorIndex)
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

    (sequenceOfQueryEncoding.map(queryEncoding => {
      val vector = new Array[Int](vectorSize)
      util.Arrays.fill(vector, 0)
      for (accCol <- queryEncoding.accessedCols) if (aggregateColToVectorIndex.get(accCol).isDefined)
        vector(aggregateColToVectorIndex.get(accCol).get) = 1
      for (groupCol <- queryEncoding.groupByKeys) if (groupByKeyToVectorIndex.get(groupCol).isDefined)
        vector(groupByKeyToVectorIndex.get(groupCol).get) = 1
      for (joinCol <- queryEncoding.joinKeys) if (joinKeyToVectorIndex.get(joinCol).isDefined)
        vector(joinKeyToVectorIndex.get(joinCol).get) = 1
      for (table <- queryEncoding.tables) if (tableToVectorIndex.get(table).isDefined)
        vector(tableToVectorIndex.get(table).get) = 1
      vec2FeatureAndFRQ.put(vector.mkString(""), (queryEncoding.accessedCols.toList.sortBy(_.toString).mkString(",") + "@"
        + queryEncoding.groupByKeys.toList.sortBy(_.toString).mkString(",") + "@" + queryEncoding.joinKeys.toList.sortBy(_.toString).mkString(",")
        + "@" + queryEncoding.tables.toList.sortBy(_.toString).mkString(","), vec2FeatureAndFRQ.getOrElse[(String, Long)](vector.mkString(""), ("", 0))._2 + 1))
      vector.mkString(",")
    }), aggregateColToVectorIndex, groupByKeyToVectorIndex, joinKeyToVectorIndex, tableToVectorIndex, aggregateColFRQ, groupByFRQ, joinKeyFRQ, tableFRQ, vec2FeatureAndFRQ)
  }

  def processToVector(processes: ListBuffer[ListBuffer[(String, Int, Int, Int, Int, Int, Int, Long, String)]], processesTest: ListBuffer[ListBuffer[(String, Int, Int, Int, Int, Int, Int, Long, String)]]
                      , sparkSession: SparkSession): (ListBuffer[ListBuffer[String]], ListBuffer[ListBuffer[String]], mutable.HashMap[String, Int], mutable.HashMap[String, Int], mutable.HashMap[String, Int], mutable.HashMap[String, Int], mutable.HashMap[String, Int], mutable.HashMap[String, Int], mutable.HashMap[String, Int], mutable.HashMap[String, Int], mutable.HashMap[String, (String, Long)]) = {

    val aggregateColFRQ = new HashMap[String, Int]()
    // val accessedColFRQ = new HashMap[String, Int]()
    val groupByFRQ = new HashMap[String, Int]()
    val joinKeyFRQ = new HashMap[String, Int]()
    val tableFRQ = new HashMap[String, Int]()
    val vec2FeatureAndFRQ = new HashMap[String, (String, Long)]()
    val aggregateColToVectorIndex = new HashMap[String, Int]()
    //  val accessedColToVectorIndex = new HashMap[String, Int]()
    val groupByKeyToVectorIndex = new HashMap[String, Int]()
    val joinKeyToVectorIndex = new HashMap[String, Int]()
    val tableToVectorIndex = new HashMap[String, Int]()
    val sequenceOfQueryEncoding = new ListBuffer[ListBuffer[QueryEncoding]]()
    val sequenceOfQueryEncodingTest = new ListBuffer[ListBuffer[QueryEncoding]]()
    val tableName: HashMap[String, String] = new HashMap()
    val hash: ListBuffer[Long] = new ListBuffer[Long]()
    for (process <- processes) {
      val processTemp = new ListBuffer[QueryEncoding]()
      var i = 0
      for (query <- process) {
        try {
          val lpp = sparkSession.sqlContext.sql(query._9).queryExecution.analyzed
          tableName.clear()
          updateAttributeName2(lpp, tableName)
          getAggSubQueries(lpp).map(lp => {
            //val aggregateCol = Paths.getAggregateColumns(lp)
            val aggregateCol = Paths.extractFilterColumns(lp).distinct

            //  val accessedCols = accessedColsSet.toSeq.distinct.sortBy(_.toString).filter(!_.contains("userDefinedColumn"))
            val joinKeys = Paths.getJoinConditions(lp).distinct.sortBy(_.toString).filter(!_.contains("userDefinedColumn"))
            val groupByKeys = getGroupByKeys(lp).distinct.sortBy(_.toString).filter(!_.contains("userDefinedColumn"))
            val tables = getTables(lp).distinct.sortBy(_.toString)
            for (col <- aggregateCol)
              aggregateColFRQ.put(col, aggregateColFRQ.getOrElse(col, 0) + 1)
            //   for (col <- accessedCols)
            //     accessedColFRQ.put(col, accessedColFRQ.getOrElse(col, 0) + 1)
            for (key <- groupByKeys)
              groupByFRQ.put(key, groupByFRQ.getOrElse(key, 0) + 1)
            for (key <- joinKeys)
              joinKeyFRQ.put(key, joinKeyFRQ.getOrElse(key, 0) + 1)
            for (table <- tables)
              tableFRQ.put(table, tableFRQ.getOrElse(table, 0) + 1)
            val gap: Int = if (i == 0) 0 else (process(i)._8 - process(i - 1)._8).toInt / (3 * 60)
            val gap2: Int = if (i == 0) 0 else (process(i)._8 - process(i - 1)._8).toInt / (1)

            //println(gap2 + "  " + gap)
            hash.+=(gap2)
            processTemp.+=(new QueryEncoding(aggregateCol, groupByKeys, joinKeys, tables, query._9, gap2))
          })
          i += 1

        }
        catch {
          case a =>
            println(a)
            println(query)
        }
      }
      // print("pp")
      if (processTemp.size > 0)
        sequenceOfQueryEncoding.+=(processTemp)
    }

    for (process <- processesTest) {
      val processTemp = new ListBuffer[QueryEncoding]()
      var i = 0
      for (query <- process) {
        try {
          val lpp = sparkSession.sqlContext.sql(query._9).queryExecution.analyzed
          tableName.clear()
          updateAttributeName2(lpp, tableName)
          getAggSubQueries(lpp).map(lp => {
            //val aggregateCol = Paths.getAggregateColumns(lp)
            val aggregateCol = Paths.extractFilterColumns(lp).distinct
            //  val accessedCols = accessedColsSet.toSeq.distinct.sortBy(_.toString).filter(!_.contains("userDefinedColumn"))
            val joinKeys = Paths.getJoinConditions(lp).distinct.sortBy(_.toString).filter(!_.contains("userDefinedColumn"))
            val groupByKeys = getGroupByKeys(lp).distinct.sortBy(_.toString).filter(!_.contains("userDefinedColumn"))
            val tables = getTables(lp).distinct.sortBy(_.toString)
            val gap2: Int = if (i == 0) 0 else (process(i)._8 - process(i - 1)._8).toInt / (1)
            processTemp.+=(new QueryEncoding(aggregateCol, groupByKeys, joinKeys, tables, query._9, gap2))
          })
          i += 1
        }
        catch {
          case a =>
            println(a)
            println(query)
        }
      }
      if (processTemp.size > 0)
        sequenceOfQueryEncodingTest.+=(processTemp)
    }


    hash.groupBy(x => x).toList.sortBy(_._1).foreach(x => println(x._1, x._2.size))
    var vectorIndex = 0

    for (col <- aggregateColFRQ.filter(_._2 >= ACCESSED_COL_MIN_FREQUENCY).map(_._1).toList.sortBy(_.toString))
      if (!aggregateColToVectorIndex.get(col).isDefined) {
        aggregateColToVectorIndex.put(col, vectorIndex)
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
    /*    var j = 0
        var g = 0
        var gg = 0
        var ggg = 0
        var jj = 0
        var jjj = 0
        sequenceOfQueryEncoding.foreach(processes => processes.foreach(queryEncoding => {
          var t = queryEncoding.groupByKeys.filter(groupCol => groupByKeyToVectorIndex.get(groupCol).isDefined).toList.size
          if (t >= 3)
            ggg += 1
          else if (t == 2)
            gg += 1
          else if (t == 1)
            g += 1
          t = queryEncoding.joinKeys.filter(joinCol => joinKeyToVectorIndex.get(joinCol).isDefined).toList.size
          if (t >= 3)
            jjj += 1
          else if (t == 2)
            jj += 1
          else if (t == 1)
            j += 1
        }))
        println(j)
        println(jj)
        println(jjj)
        println(g)
        println(gg)
        println(ggg)
        throw new Exception*/
    (sequenceOfQueryEncoding.map(processes => processes.map(queryEncoding => {
      val vector = new Array[Int](vectorSize/* + 20*/ + reserveFeature)
      util.Arrays.fill(vector, 0)
      for (accCol <- queryEncoding.accessedCols) if (aggregateColToVectorIndex.get(accCol).isDefined)
        vector(aggregateColToVectorIndex.get(accCol).get) = 1
      for (groupCol <- queryEncoding.groupByKeys) if (groupByKeyToVectorIndex.get(groupCol).isDefined)
        vector(groupByKeyToVectorIndex.get(groupCol).get) = 1
      for (joinCol <- queryEncoding.joinKeys) if (joinKeyToVectorIndex.get(joinCol).isDefined)
        vector(joinKeyToVectorIndex.get(joinCol).get) = 1
      for (table <- queryEncoding.tables) if (tableToVectorIndex.get(table).isDefined)
        vector(tableToVectorIndex.get(table).get) = 1
      //vector(vectorSize + gabToIndex(queryEncoding.date)) = 1
      vec2FeatureAndFRQ.put(vector.mkString(""), (queryEncoding.accessedCols.toList.sortBy(_.toString).mkString(",") + "@"
        + queryEncoding.groupByKeys.toList.sortBy(_.toString).mkString(",") + "@" + queryEncoding.joinKeys.toList.sortBy(_.toString).mkString(",")
        + "@" + queryEncoding.tables.toList.sortBy(_.toString).mkString(","), vec2FeatureAndFRQ.getOrElse[(String, Long)](vector.mkString(""), ("", 0))._2 + 1))
      vector.mkString("")
    }))
      , sequenceOfQueryEncodingTest.map(processes => processes.map(queryEncoding => {
      val vector = new Array[Int](vectorSize /*+ 20 */ +reserveFeature)
      util.Arrays.fill(vector, 0)
      for (accCol <- queryEncoding.accessedCols) if (aggregateColToVectorIndex.get(accCol).isDefined)
        vector(aggregateColToVectorIndex.get(accCol).get) = 1
      for (groupCol <- queryEncoding.groupByKeys) if (groupByKeyToVectorIndex.get(groupCol).isDefined)
        vector(groupByKeyToVectorIndex.get(groupCol).get) = 1
      for (joinCol <- queryEncoding.joinKeys) if (joinKeyToVectorIndex.get(joinCol).isDefined)
        vector(joinKeyToVectorIndex.get(joinCol).get) = 1
      for (table <- queryEncoding.tables) if (tableToVectorIndex.get(table).isDefined)
        vector(tableToVectorIndex.get(table).get) = 1
      //vector(vectorSize + gabToIndex(queryEncoding.date)) = 1
      vec2FeatureAndFRQ.put(vector.mkString(""), (queryEncoding.accessedCols.toList.sortBy(_.toString).mkString(",") + "@"
        + queryEncoding.groupByKeys.toList.sortBy(_.toString).mkString(",") + "@" + queryEncoding.joinKeys.toList.sortBy(_.toString).mkString(",")
        + "@" + queryEncoding.tables.toList.sortBy(_.toString).mkString(","), vec2FeatureAndFRQ.getOrElse[(String, Long)](vector.mkString(""), ("", 0))._2 + 1))
      vector.mkString("")
    })), aggregateColToVectorIndex, groupByKeyToVectorIndex, joinKeyToVectorIndex, tableToVectorIndex, aggregateColFRQ, groupByFRQ, joinKeyFRQ, tableFRQ, vec2FeatureAndFRQ)

  }

  def getTables(lp: LogicalPlan): Seq[String] = lp match {
    case SubqueryAlias(identifier, child@LogicalRDD(output, rdd, outputPartitioning, outputOrdering, isStreaming)) =>
      Seq(identifier.identifier.toLowerCase)
    case t =>
      t.children.flatMap(getTables)
  }

  def gabToIndex(gap: Long): Int = {
    /* if (0 <= gap && gap < 1)
       0
     else if (1 <= gap && gap < 2)
       1
     else if (2 <= gap && gap < 4)
       2
     else if (4 <= gap && gap < 6)
       3
     else if (6 <= gap && gap < 10)
       4
     else if (10 <= gap && gap < 14)
       5
     else if (14 <= gap && gap < 22)
       6
     else if (22 <= gap && gap < 30)
       7
     else if (30 <= gap && gap < 46)
       8
     else if (46 <= gap && gap < 62)
       9
     else if (60 <= gap && gap < 94)
       10
     else if (94 <= gap && gap < 126)
       11
     else if (126 <= gap && gap < 190)
       12
     else if (190 <= gap && gap < 254)
       13
     else if (254 <= gap && gap < 383)
       14
     else if (383 <= gap && gap < 511)
       15
     else if (511 <= gap && gap < 772)
       16
     else if (772 <= gap && gap < 1069)
       17
     else if (1069 <= gap && gap < 1440)
       18
     else // if (800 <= gap)
       19*/
    if (0 <= gap && gap < 1)
      0
    else if (1 <= gap && gap < 2)
      1
    else if (2 <= gap && gap < 3)
      2
    else if (3 <= gap && gap < 6)
      3
    else if (6 <= gap && gap < 9)
      4
    else if (9 <= gap && gap < 12)
      5
    else if (12 <= gap && gap < 16)
      6
    else if (16 <= gap && gap < 26)
      7
    else if (26 <= gap && gap < 55)
      8
    else if (55 <= gap && gap < 100)
      9
    else if (100 <= gap && gap < 200)
      10
    else if (200 <= gap && gap < 400)
      11
    else if (400 <= gap && gap < 800)
      12
    else // if (800 <= gap)
      13
  }


  def readRDDScanRowCNT(sparkSession: SparkSession): mutable.HashMap[String, Long] = {
    val foldersOfParquetTable = new File(pathToTableParquet).listFiles.filter(x => x.isDirectory && x.getName.contains(".parquet") && !x.getName.contains("sample"))
    val tables = sparkSession.sessionState.catalog.listTables("default").map(t => t.table)
    val map = mutable.HashMap[String, Long]()
    foldersOfParquetTable.filter(x => tables.contains(x.getName.split("\\.")(0).toLowerCase)).map(folder => {
      val lRDD = sparkSession.sessionState.catalog.lookupRelation(org.apache.spark.sql.catalyst.TableIdentifier
      (folder.getName.split("\\.")(0), None)).children(0).asInstanceOf[LogicalRDD]
      (RDDScanExec(lRDD.output, lRDD.rdd, "ExistingRDD", lRDD.outputPartitioning, lRDD.outputOrdering), folderSize(folder) /* Paths.tableToCount.get(folder.getName.split("\\.")(0).toLowerCase).get*/ , folder.getName.split("\\.")(0).toLowerCase)
    }).foreach(x => map.put(x._1.output.map(o => x._3 + "." + o.name.split(delimiterSparkSQLNameAndID)(0)).mkString(delimiterSynopsisFileNameAtt).toLowerCase, x._2))
    map
  }


  def folderSize(directory: File): Long = {
    var length: Long = 0
    for (file <- directory.listFiles) {
      if (file.isFile) length += file.length
      else length += folderSize(file)
    }
    length / 100000
  }


  def updateAttributeName(lp: LogicalPlan, tableFrequency: mutable.HashMap[String, Int]): Unit = lp match {
    case SubqueryAlias(identifier, child@LogicalRDD(output, rdd, outputPartitioning, outputOrdering, isStreaming)) =>
      val att = output.toList
      tableFrequency.put(identifier.identifier, tableFrequency.getOrElse(identifier.identifier, 0) + 1)
      for (i <- 0 to output.size - 1)
        tableName.put(att(i).toAttribute.toString().toLowerCase, identifier.identifier + "_" + tableFrequency.get(identifier.identifier).get)
    case a =>
      a.children.foreach(x => updateAttributeName(x, tableFrequency))
  }

  def updateAttributeName2(lp: LogicalPlan, tableName: mutable.HashMap[String, String]): Unit = lp match {
    case SubqueryAlias(identifier, child@LogicalRDD(output, rdd, outputPartitioning, outputOrdering, isStreaming)) =>
      val att = output.toList
      for (i <- 0 to output.size - 1)
        tableName.put(att(i).toAttribute.toString().toLowerCase, (identifier.identifier).toLowerCase)
    case a =>
      a.children.foreach(x => updateAttributeName(x))
  }


  def getAggSubQueries(lp: LogicalPlan): Seq[LogicalPlan] = lp match {

    case a@Aggregate(groupingExpressions, aggregateExpressions, child) =>
      Seq(a) ++ getAggSubQueries(child)
    case a =>
      a.children.flatMap(child => getAggSubQueries(child))
  }


  def setStaticFraction(confidence: Double, error: Double): Double =
    if (confidence >= 1.0 || confidence == 0)
      1.0
    else if (confidence >= .99 && error <= .10)
      .50
    else if (confidence >= .90 && error <= .10)
      .30
    else if (confidence >= .90 && error <= .20)
      .20
    else if (confidence >= .80 && error <= .20)
      0.15
    else
      0.1

  //todo set better p and m
  def hashString(string: String): Long = {
    val p = 53;
    val m = 1e9 + 9;
    var hash_value = 0;
    var p_pow = 1;
    for (c <- string) {
      hash_value = ((hash_value + (c - 'a' + 1) * p_pow) % m).toInt;
      p_pow = ((p_pow * p) % m).toInt;
    }
    hash_value
  }

  def updateWareHouseLRU: Unit = {
    val warehouseSynopsesToSize = warehouseParquetNameToSize.map(x => (ParquetNameToSynopses(x._1), x._2))
    if (warehouseSynopsesToSize.size == 0 || warehouseSynopsesToSize.reduce((a, b) => (null, a._2 + b._2))._2 <= maxSpace * 10)
      return
    var candidateSynopsesSize: Long = 0
    val p = lastUsedOfParquetSample.toList.sortBy(_._2).reverse
    var index = 0
    var bestSynopsis = p(index)
    var bestSynopsisSize = warehouseSynopsesToSize.getOrElse(ParquetNameToSynopses(bestSynopsis._1), 0.toLong)
    val removeSynopses = new mutable.HashSet[String]()
    while (index < warehouseSynopsesToSize.size - 1) {
      if (candidateSynopsesSize + bestSynopsisSize < maxSpace * 10) {
        candidateSynopsesSize += bestSynopsisSize
        index += 1
        bestSynopsis = p(index)
        bestSynopsisSize = warehouseSynopsesToSize.getOrElse(ParquetNameToSynopses(bestSynopsis._1), 0.toLong)
      }
      else {
        removeSynopses.add(bestSynopsis._1)
        index += 1
        bestSynopsis = p(index)
        bestSynopsisSize = warehouseSynopsesToSize.getOrElse(ParquetNameToSynopses(bestSynopsis._1), 0.toLong)
      }
    }
    removeSynopses.foreach(x => {
      (Directory(new File(pathToSaveSynopses + x + ".obj"))).deleteRecursively()
      warehouseParquetNameToSize.remove(x)
      SynopsesToParquetName.remove(ParquetNameToSynopses.getOrElse(x, "null"))
      ParquetNameToSynopses.remove(x)
      parquetNameToHeader.remove(x)
      lastUsedOfParquetSample.remove(x)
      //System.err.println("removed:    " + x)
    })
  }


  def extractSynopses(pp: SparkPlan): Seq[UnaryExecNode] = pp match {
    case s: SampleExec =>
      Seq(s)
    case s: SketchExec =>
      Seq(s)
    case a =>
      a.children.flatMap(extractSynopses)
  }


}