package definition
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.LeafNode
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ComplexTypeMergingExpression, NamedExpression, UnaryExpression, _}
import sketch.Sketch
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project, Sort, SubqueryAlias}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.sources

import scala.collection.{Seq, mutable}

object  Paths {
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
  var fraction = 1.0
  val start = 0
  val testSize = 350
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

  val ACCESSED_COL_MIN_FREQUENCY = 150
  val GROUPBY_COL_MIN_FREQUENCY = 70
  val JOIN_COL_MIN_FREQUENCY = 70
  val MAX_NUMBER_OF_QUERY_REPETITION = 100000


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
      getJoinConditions(condition)
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
        Seq(getAccessedColsOfExpression(e.left).mkString(",") + "=" + getAccessedColsOfExpression(e.right).mkString(","))
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

}
