package costModel

import definition.Paths
import definition.Paths.{delimiterParquetColumn, delimiterSynopsesColumnName, delimiterSynopsisFileNameAtt, getAccessedColsOfExpressions, getHeaderOfOutput, mapRDDScanRowCNT, tableName, _}
import operators.physical._
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, _}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.execution.{SampleExec => _, _}

import scala.collection.mutable.ListBuffer
import scala.collection.{Seq, mutable}

abstract class CostModelAbs() extends Serializable {

  val maxSpace = Paths.maxSpace * 10
  //var future: Seq[Seq[Seq[SparkPlan]]]
  val costOfProject: Long = Paths.costOfProject
  val costOfScan: Long = Paths.costOfScan
  val costOfJoin: Long = Paths.costOfJoin
  val costOfShuffle: Long = Paths.costOfShuffle
  val costOfUniformSample: Long = Paths.costOfUniformSample
  val costOfUniversalSample: Long = Paths.costOfUniversalSample
  val costOfDistinctSample: Long = Paths.costOfDistinctSample

  val costOfFilter: Long = 0
  val filterRatio: Double = 1.0
  val costOfUniformWithoutCISample: Long = 1
  val costOfScale: Long = 0
  val HashAggregate: Long = 0
  val SortAggregate: Long = 0
  val setSelectionStrategy: BestSetSelectorAbs

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def AreCovered(samples: Seq[SampleExec], w: Seq[String]): Boolean = {
    if (samples.size == 0) return false
    if (w.size==0) return false
    samples.foreach(sample => {
      if (!w.find(y => isMoreAccurate(sample, y)).isDefined)
        return false
    })
    return true
  }

  def getFutureProjectList(): Seq[String] = Seq()

  def updateWarehouse(): Unit

  def getWRSynopsesSize = warehouseParquetNameToSize.map(x => (ParquetNameToSynopses(x._1), x._2))

  def getWRSynopsesRowCNT = warehouseParquetNameToRow.map(x => (ParquetNameToSynopses(x._1), x._2))

  def getSizeOfAtt(in: Seq[Attribute]) = in.map(x => x.dataType.defaultSize).reduce(_ + _)

  def getCheapestExactCostOf(queries: Seq[Seq[Seq[SparkPlan]]]): Long = {
    queries.map(query => query.map(subQueryAPPs => subQueryAPPs.map(APP => costOfExact(APP)._2).min).reduce(_ + _)).reduce(_ + _)
  }

  def getCheapestExactCostOf(pp: SparkPlan): Long = {
    costOfExact(pp)._2
  }

  def getCheapestExactCostOf(queries: Seq[Seq[Seq[SparkPlan]]], pp: SparkPlan): Long = {
    if (queries.size == 0)
      return getCheapestExactCostOf(pp)
    getCheapestExactCostOf(queries) + getCheapestExactCostOf(pp) //queries.map(query => query.map(subQueryAPPs => subQueryAPPs.map(APP => costOfExact(APP)._2).min).reduce(_ + _)).reduce(_ + _)
  }

  def getFutureSize(): Long

  def getFutureAPP(): Seq[Seq[Seq[SparkPlan]]]

  def suggest(): Seq[SparkPlan]

  def addQuery(query: String, ip: String, epoch: Long, f: Seq[String] = null): Unit

  def coverage(s: Seq[UnaryExecNode], b: Seq[UnaryExecNode]): Int = {
    if (b.map(bS => s.find(ss => isMoreAccurate(ss.asInstanceOf[SampleExec], bS.toString())).isDefined).reduce(_ && _)) 1 else 0
  }


  def extractSynopses(pp: SparkPlan): Seq[UnaryExecNode] = pp match {
    case s: SampleExec =>
      Seq(s)
    case s: SketchExec =>
      Seq(s)
    case a =>
      a.children.flatMap(extractSynopses)
  }

  def enumerateRawPlanWithJoin(rawPlan: LogicalPlan): Seq[LogicalPlan] = {
    var rootTemp: ListBuffer[LogicalPlan] = new ListBuffer[LogicalPlan]
    val subQueries = new ListBuffer[SubqueryAlias]()
    val queue = new mutable.Queue[LogicalPlan]()
    var joinConditions: ListBuffer[BinaryExpression] = new ListBuffer[BinaryExpression]()
    queue.enqueue(rawPlan)
    while (!queue.isEmpty) {
      val t = queue.dequeue()
      t match {
        case SubqueryAlias(name, child) =>
          subQueries.+=(t.asInstanceOf[SubqueryAlias])
        case Aggregate(groupingExpressions, aggregateExpressions, child) =>
          queue.enqueue(child)
          rootTemp.+=(Aggregate(groupingExpressions, aggregateExpressions, child))
        case Filter(conditions, child) =>
          joinConditions.++=(getJoinConditions(conditions))
          queue.enqueue(child)
          rootTemp.+=(Filter(conditions, rawPlan.children(0).children(0)))
        case org.apache.spark.sql.catalyst.plans.logical.Join(left: LogicalPlan, right: LogicalPlan, joinType: JoinType,
        condition: Option[Expression]) =>
          queue.enqueue(left)
          queue.enqueue(right)
          if (condition.isDefined)
            joinConditions.+=(condition.get.asInstanceOf[BinaryExpression])
        case Project(p, c) =>
          queue.enqueue(c)
          rootTemp.+=(Project(p, rawPlan.children(0).children(0)))
        case Sort(order: Seq[SortOrder], global: Boolean, child: LogicalPlan) =>
          queue.enqueue(child)
          rootTemp += Sort(order, global, rawPlan.children(0).children(0))
        case u@UnresolvedRelation(table) =>
          subQueries.+=(SubqueryAlias(AliasIdentifier(table.table), u))
        case _ =>
          throw new Exception("new logical raw node")
      }
    }
    var allPossibleJoinOrders = new ListBuffer[Seq[LogicalPlan]]
    allPossibleJoinOrders.+=(Seq())
    allPossibleJoinOrders.+=(subQueries)
    for (i <- 2 to subQueries.size) {
      val temp = new ListBuffer[Join]
      for (j <- (1 to i - 1))
        if (j == i - j) {
          for (l <- 0 to allPossibleJoinOrders(j).size - 1)
            for (r <- l + 1 to allPossibleJoinOrders(j).size - 1)
              if (!hasTheSameSubquery(allPossibleJoinOrders(j)(l), allPossibleJoinOrders(j)(r))
                && IsJoinable(allPossibleJoinOrders(j)(l), allPossibleJoinOrders(j)(r), joinConditions))
                temp.+=:(Join(allPossibleJoinOrders(j)(l), allPossibleJoinOrders(j)(r), org.apache.spark.sql.catalyst.plans.Inner, Some(chooseConditionFor(allPossibleJoinOrders(j)(l), allPossibleJoinOrders(j)(r), joinConditions))))
        } else if (j < i - j) {
          for (left <- allPossibleJoinOrders(j))
            for (right <- allPossibleJoinOrders(i - j))
              if (!hasTheSameSubquery(left, right) && IsJoinable(left, right, joinConditions)) {
                val jj = (Join(left, right, org.apache.spark.sql.catalyst.plans.Inner, Some(chooseConditionFor(left, right, joinConditions))))
                temp.+=:(jj)
              }
        }
      allPossibleJoinOrders.+=(temp)
    }
    var plans = new ListBuffer[LogicalPlan]()
    for (j <- 0 to allPossibleJoinOrders(subQueries.size).size - 1) {
      plans.+=(allPossibleJoinOrders(subQueries.size)(j))
      for (i <- rootTemp.size - 1 to 0 by -1) {
        rootTemp(i) match {
          case Aggregate(groupingExpressions, aggregateExpressions, child) =>
            plans(j) = Aggregate(groupingExpressions, aggregateExpressions, plans(j))
          case Filter(condition, child) =>
            plans(j) = Filter(condition, plans(j))
          case Project(p, c) =>
            plans(j) = Project(p, plans(j))
          case Sort(o, g, c) =>
            plans(j) = Sort(o, g, plans(j))
          case _ =>
            throw new Exception("new logical node")
        }
      }
    }
    plans
  }

  def chooseConditionFor(lp1: LogicalPlan, lp2: LogicalPlan, joinConditions: ListBuffer[BinaryExpression]): Expression = {
    for (i <- 0 to joinConditions.length - 1) {
      val lJoinkey = Set(joinConditions(i).left.find(_.isInstanceOf[AttributeReference]).get.toString().toLowerCase)
      val rJoinkey = Set(joinConditions(i).right.find(_.isInstanceOf[AttributeReference]).get.toString().toLowerCase)
      if ((lJoinkey.subsetOf(lp1.output.map(_.toAttribute.toString().toLowerCase).toSet) && rJoinkey.subsetOf(lp2.output.map(_.toAttribute.toString().toLowerCase).toSet))
        || (lJoinkey.subsetOf(lp2.output.map(_.toAttribute.toString().toLowerCase).toSet) && rJoinkey.subsetOf(lp1.output.map(_.toAttribute.toString().toLowerCase).toSet)))
        return joinConditions(i).asInstanceOf[Expression]
    }
    throw new Exception("Cannot find any condition to join the two tables")
  }

  def getJoinConditions(exp: Expression): Seq[BinaryExpression] = exp match {
    case And(left, right) =>
      getJoinConditions(left) ++ getJoinConditions(right)
    case Or(left, right) =>
      getJoinConditions(left) ++ getJoinConditions(right)
    case b@BinaryOperator(left, right) =>
      if (left.find(_.isInstanceOf[AttributeReference]).isDefined && right.find(_.isInstanceOf[AttributeReference]).isDefined)
        return Seq(b)
      Seq()
    case _ =>
      Seq()
  }

  def IsJoinable(lp1: LogicalPlan, lp2: LogicalPlan, joinConditions: ListBuffer[BinaryExpression]): Boolean = {
    for (i <- 0 to joinConditions.length - 1) {
      val lJoinkey = Set(joinConditions(i).left.find(_.isInstanceOf[AttributeReference]).get.toString().toLowerCase)
      val rJoinkey = Set(joinConditions(i).right.find(_.isInstanceOf[AttributeReference]).get.toString().toLowerCase)
      if ((lJoinkey.subsetOf(lp1.output.map(_.toAttribute.toString().toLowerCase).toSet) && rJoinkey.subsetOf(lp2.output.map(_.toAttribute.toString().toLowerCase).toSet))
        || (lJoinkey.subsetOf(lp2.output.map(_.toAttribute.toString().toLowerCase).toSet) && rJoinkey.subsetOf(lp1.output.map(_.toAttribute.toString().toLowerCase).toSet)))
        return true
    }
    false
  }

  def hasTheSameSubquery(plan1: LogicalPlan, plan2: LogicalPlan): Boolean = {
    val queue = new mutable.Queue[LogicalPlan]()
    val subquery = new ListBuffer[AliasIdentifier]
    queue.enqueue(plan2)
    while (!queue.isEmpty) {
      val t = queue.dequeue()
      t match {
        case SubqueryAlias(name, child) =>
          subquery.+=(name)
        case _ =>
          t.children.foreach(x => queue.enqueue(x))
      }
    }
    hasSubquery(plan1, subquery)
  }

  def hasSubquery(plan: LogicalPlan, query: Seq[AliasIdentifier]): Boolean = {

    plan match {
      case SubqueryAlias(name: AliasIdentifier, child: LogicalPlan) =>
        if (query.contains(name))
          return true
      case _ =>
        plan.children.foreach(x => if (hasSubquery(x, query) == true) return true)
    }
    false
  }

  //if sampleInfo>samlpleExec => true
  def isMoreAccurate(sampleExec: SampleExec, sampleInf: String): Boolean = sampleExec match {
    case u@UniformSampleExec2(functions, confidence, error, seed, child) =>
      val sampleInfo = sampleInf.split(delimiterSynopsisFileNameAtt)
      if (sampleInfo(0).equals("Uniform") && (if (u.output.size == 0) {
        val tttt = tableName.get(u.child.asInstanceOf[ProjectExec].child.output(0).toString().toLowerCase).get.split("_")(0)
        if (sampleInfo(1).split(delimiterParquetColumn).map(_.split("\\.")(0).split("_")(0)).find(_.equalsIgnoreCase(tttt)).isDefined)
          true
        else false
      } else getHeaderOfOutput(u.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet))
        && sampleInfo(2).toDouble >= confidence && sampleInfo(3).toDouble <= error
        //     && functions.map(_.toString()).toSet.subsetOf(sampleInfo(5).split(delimiterSynopsesColumnName).toSet)
        && sampleInfo(7).equals(u.joins))
        return true
      else if (sampleInfo(0).equals("Universal") && getHeaderOfOutput(u.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet)
        && sampleInfo(2).toDouble >= confidence && sampleInfo(3).toDouble <= error
        //   && functions.map(_.toString()).toSet.subsetOf(sampleInfo(5).split(delimiterSynopsesColumnName).toSet)
        && sampleInfo(7).equals(u.joins))
        return true
      else if (sampleInfo(0).equals("Distinct") && getHeaderOfOutput(u.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet)
        && sampleInfo(2).toDouble >= confidence && sampleInfo(3).toDouble <= error
        //   && functions.map(_.toString()).toSet.subsetOf(sampleInfo(6).split(delimiterSynopsesColumnName).toSet)
        && sampleInfo(8).equals(u.joins))
        return true
      else false
    case d@DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, child) =>
      val sampleInfo = sampleInf.split(delimiterSynopsisFileNameAtt)
      if (sampleInfo(0).equals("Distinct")
        && getHeaderOfOutput(d.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet)
        && sampleInfo(2).toDouble >= confidence && sampleInfo(3).toDouble <= error
        //   && functions.map(_.toString()).toSet.subsetOf(sampleInfo(6).split(delimiterSynopsesColumnName).toSet)
        && getAccessedColsOfExpressions(groupingExpressions).toSet.subsetOf(sampleInfo(7).split(delimiterSynopsesColumnName).toSet)
        && sampleInfo(8).equals(d.joins))
        true else
        false
    case u@UniversalSampleExec2(functions, confidence, error, seed, joinKey, child) =>
      val sampleInfo = sampleInf.split(delimiterSynopsisFileNameAtt)
      if (sampleInfo(0).equals("Universal") && getHeaderOfOutput(u.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet)
        && sampleInfo(2).toDouble >= confidence && sampleInfo(3).toDouble <= error
        //   && functions.map(_.toString()).toSet.subsetOf(sampleInfo(5).split(delimiterSynopsesColumnName).toSet)
        && sampleInfo(7).equals(u.joins)
        && getAccessedColsOfExpressions(joinKey).toSet.subsetOf(sampleInfo(6).split(delimiterSynopsesColumnName).toSet))
        true
      else
        false
    case u@UniformSampleExec2WithoutCI(seed, child) =>
      val sampleInfo = sampleInf.split(delimiterSynopsisFileNameAtt)
      if (sampleInfo(0).equals("UniformWithoutCI") && getHeaderOfOutput(u.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet)
        && sampleInfo(7).equals(u.joins))
        true else
        false
    case _ => false
  }

  def getTableColumnsName(output: Seq[Attribute]): Seq[String] =
    output.map(o => tableName.get(o.toString().toLowerCase).get.split("\\.")(0).dropRight(2) + "." + o.name.split("#")(0).toLowerCase)

  def costOfExact(pp: SparkPlan): (Long, Long) = pp match { //(#row,CostOfPlan)
    case s: SampleExec =>
      costOfExact(s.child)
    case f: FilterExec =>
      costOfExact(f.child)
    case ProjectExec(projectList, child) =>
      val (inputSize, childCost) = costOfExact(child)
      val projectRatio = (if (projectList.size == 0) 1 else projectList.size )/ child.output.size.toDouble
      ((projectRatio * inputSize).toLong + 1, costOfProject * inputSize + childCost)
    case SortMergeJoinExec(a, b, c, d, left, right) =>
      val (leftInputSize, leftChildCost) = costOfExact(left)
      val (rightInputSize, rightChildCost) = costOfExact(right)
      val outputSize = math.min(leftInputSize, rightInputSize)
      (outputSize, costOfJoin * (leftInputSize + rightInputSize) + costOfShuffle * (leftInputSize * (math.log(leftInputSize) / math.log(2)) + rightInputSize * (math.log(rightInputSize) / math.log(2))).toLong + leftChildCost + rightChildCost)
    case r: RDDScanExec =>
      val size: Long = mapRDDScanRowCNT.getOrElse(getTableColumnsName(r.output).mkString(";").toLowerCase, -1)
      if (size == -1)
        throw new Exception("The size does not exist: " + r.toString())
      (size, costOfScan * size)
    case s: ScaleAggregateSampleExec =>
      costOfExact(s.child)
    case s: SketchExec =>
      throw new Exception("No cost is defined for the node")
    case h: HashAggregateExec =>
      val (rowCNT, childCost) = costOfExact(h.child)
      (rowCNT, HashAggregate * rowCNT + childCost)
    case h: SortAggregateExec =>
      val (rowCNT, childCost) = costOfExact(h.child)
      (rowCNT, SortAggregate * rowCNT + childCost)
    case h: Exchange =>
      costOfExact(h.child)
    case x: XXX =>
      costOfExact(x.child)
    case w: WeightCombiner =>
      costOfExact(w.child)
    case _ =>
      throw new Exception("No cost is defined for the node")
  }

  def costOfAppWithFixedSynopses(pp: SparkPlan, synopsesCost: Seq[(String, Long)]): (Long, Long) = pp match { //(#row,CostOfPlan)
    case s: SampleExec =>
      val sample = synopsesCost.find(x => isMoreAccurate(s, x._1)).map(x => (x._2, costOfScan * x._2))
      if (sample.isDefined)
        sample.get
      else
        costOfAppWithFixedSynopses(s.child, synopsesCost)
    case f: FilterExec =>
      costOfAppWithFixedSynopses(f.child, synopsesCost)
    case ProjectExec(projectList, child) =>
      val (inputSize, childCost) = costOfAppWithFixedSynopses(child, synopsesCost)
      val projectRatio = (if (projectList.size == 0) 1 else projectList.size )/ child.output.size.toDouble
      ((projectRatio * inputSize).toLong + 1, costOfProject * inputSize + childCost)
    case SortMergeJoinExec(a, b, c, d, left, right) =>
      val (leftInputSize, leftChildCost) = costOfAppWithFixedSynopses(left, synopsesCost)
      val (rightInputSize, rightChildCost) = costOfAppWithFixedSynopses(right, synopsesCost)
      val outputSize = math.min(leftInputSize, rightInputSize)
      (outputSize, costOfJoin * (leftInputSize + rightInputSize) + costOfShuffle * (leftInputSize * (math.log(leftInputSize) / math.log(2)) + rightInputSize * (math.log(rightInputSize) / math.log(2))).toLong + leftChildCost + rightChildCost)
    case l: RDDScanExec =>
      val size: Long = mapRDDScanRowCNT.getOrElse(getTableColumnsName(l.output).mkString(";").toLowerCase, -1)
      if (size == -1)
        throw new Exception("The size does not exist: " + l.toString())
      (size, costOfScan * size)
    case s: ScaleAggregateSampleExec =>
      val (rowCNT, childCost) = costOfAppWithFixedSynopses(s.child, synopsesCost)
      (rowCNT, costOfScale * rowCNT + childCost)
    case h: HashAggregateExec =>
      val (rowCNT, childCost) = costOfAppWithFixedSynopses(h.child, synopsesCost)
      (rowCNT, HashAggregate * rowCNT + childCost)
    case h: SortAggregateExec =>
      val (rowCNT, childCost) = costOfAppWithFixedSynopses(h.child, synopsesCost)
      (rowCNT, SortAggregate * rowCNT + childCost)
    case x: XXX =>
      costOfAppWithFixedSynopses(x.child, synopsesCost)
    case x: WeightCombiner =>
      costOfAppWithFixedSynopses(x.child, synopsesCost)
    case _ =>
      throw new Exception("No cost is defined for the node")
  }

  def costOfAppPlan(pp: SparkPlan, synopsesCost: Seq[(String, Long)]): (Long, Long) = pp match { //(#row,CostOfPlan)
    case f: FilterExec =>
      costOfAppPlan(f.child, synopsesCost)
    case ProjectExec(projectList, child) =>
      val (inputSize, childCost) = costOfAppPlan(child, synopsesCost)
      val projectRatio = (if (projectList.size == 0) 1 else projectList.size )/ child.output.size.toDouble
      ((projectRatio * inputSize).toLong + 1, costOfProject * inputSize + childCost)
    case SortMergeJoinExec(a, b, c, d, left, right) =>
      val (leftInputSize, leftChildCost) = costOfAppPlan(left, synopsesCost)
      val (rightInputSize, rightChildCost) = costOfAppPlan(right, synopsesCost)
      val outputSize = math.min(leftInputSize, rightInputSize)
      (outputSize, costOfJoin * (leftInputSize + rightInputSize) + costOfShuffle * (leftInputSize * (math.log(leftInputSize) / math.log(2)) + rightInputSize * (math.log(rightInputSize) / math.log(2))).toLong + leftChildCost + rightChildCost)
    case l: RDDScanExec =>
      val size: Long = mapRDDScanRowCNT.getOrElse(getTableColumnsName(l.output).mkString(";").toLowerCase, -1)
      if (size == -1)
        throw new Exception("The size does not exist: " + l.toString())
      (size, costOfScan * size)
    case s: UniformSampleExec2 =>
      synopsesCost.find(x => isMoreAccurate(s, x._1)).map(x => (x._2, costOfScan * x._2)).getOrElse({
        val inputSize = synopsesSize.getOrElse(s.toString(), costOfAppPlan(s.child, synopsesCost)._1)
        val childCost = costOfAppPlan(s.child, synopsesCost)._2
        if (s.fraction >= 1.0)
          (inputSize, 1 * inputSize + childCost)
        else
          ((s.fraction * inputSize).toLong, costOfUniformSample * inputSize + childCost)
      })
    case s: DistinctSampleExec2 =>
      synopsesCost.find(x => isMoreAccurate(s, x._1)).map(x => (x._2, costOfScan * x._2)).getOrElse({
        val inputSize = synopsesSize.getOrElse(s.toString(), costOfAppPlan(s.child, synopsesCost)._1)
        val childCost = costOfAppPlan(s.child, synopsesCost)._2
        if (s.fraction >= 1.0)
          (inputSize, 1 * inputSize + childCost)
        else
          ((s.fraction * inputSize).toLong, costOfDistinctSample * inputSize + childCost)
      })
    case s: UniversalSampleExec2 =>
      synopsesCost.find(x => isMoreAccurate(s, x._1)).map(x => (x._2, costOfScan * x._2)).getOrElse({
        val inputSize = synopsesSize.getOrElse(s.toString(), costOfAppPlan(s.child, synopsesCost)._1)
        val childCost = costOfAppPlan(s.child, synopsesCost)._2
        if (s.fraction >= 1.0)
          (inputSize, 1 * inputSize + childCost)
        else
          ((s.fraction * inputSize).toLong, costOfUniversalSample * inputSize + childCost)
      })
    case s: UniformSampleExec2WithoutCI =>
      synopsesCost.find(x => isMoreAccurate(s, x._1)).map(x => (x._2, costOfScan * x._2)).getOrElse({
        val (inputSize, childCost) = costOfAppPlan(s.child, synopsesCost)
        if (s.fraction >= 1.0)
          (inputSize, 1 * inputSize + childCost)
        else
          ((s.fraction * inputSize).toLong, costOfUniformWithoutCISample * inputSize + childCost)
      })
    case s: ScaleAggregateSampleExec =>
      val (rowCNT, childCost) = costOfAppPlan(s.child, synopsesCost)
      (rowCNT, costOfScale * rowCNT + childCost)
    case h: HashAggregateExec =>
      val (rowCNT, childCost) = costOfAppPlan(h.child, synopsesCost)
      (rowCNT, HashAggregate * rowCNT + childCost)
    case h: SortAggregateExec =>
      val (rowCNT, childCost) = costOfAppPlan(h.child, synopsesCost)
      (rowCNT, SortAggregate * rowCNT + childCost)
    case x: XXX =>
      costOfAppPlan(x.child, synopsesCost)
    case x: WeightCombiner =>
      costOfAppPlan(x.child, synopsesCost)
    case x: ShuffleExchangeExec =>
      costOfAppPlan(x.child, synopsesCost)
    case x: SortExec =>
      costOfAppPlan(x.child, synopsesCost)
    case _ =>
      throw new Exception("No cost is defined for the node")
  }

  def toStringTree(pp: SparkPlan, intent: String = ""): String = pp match {
    case s: ScaleAggregateSampleExec =>
      "" + toStringTree(s.child, intent)
    case s: FilterExec =>
      "" + toStringTree(s.child, intent) // intent + "FFFF\n" + pp.children.map(child => toStringTree(child, intent + "     ")).mkString("")
    case s: ProjectExec =>
      "" + toStringTree(s.child, intent) //intent + "PPPP(" + getHeaderOfOutput(p.projectList.map(_.asInstanceOf[Attribute])) + ")\n" + toStringTree(p.child, intent + "     ")
    case s: RDDScanExec =>
      val table = tableName.get(s.output(0).toString().toLowerCase).get.split("\\.")(0).dropRight(2)
      intent + "SSSS(" + table + ")\n"
    case s: SortMergeJoinExec =>
      intent + "JJJJ(" + getHeaderOfOutput(s.leftKeys.find(_.isInstanceOf[AttributeReference]).get.map(_.asInstanceOf[AttributeReference])) + "=" + getHeaderOfOutput(s.rightKeys.find(_.isInstanceOf[AttributeReference]).get.map(_.asInstanceOf[AttributeReference])) + ")\n" + pp.children.map(child => toStringTree(child, intent + "     ")).mkString("")
    case HashAggregateExec(requiredChildDistributionExpressions, groupingExpressions, aggregateExpressions, aggregateAttributes, initialInputBufferOffset, resultExpressions, child: HashAggregateExec) =>
      intent + "AAAA(" + aggToString(aggregateExpressions) + ", GroupBy(" + getHeaderOfOutput(groupingExpressions.map(_.asInstanceOf[Attribute])) + "))\n" + toStringTree(child, intent + "     ")
    case s: HashAggregateExec =>
      "" + toStringTree(s.child, intent)
    case s: UniformSampleExec2 =>
      intent + "UNIF(" + aggToString(s.functions) + ", fraction=" + s.fraction + ")\n" + toStringTree(s.child, intent + "     ")
    case s: UniversalSampleExec2 =>
      intent + "UNIV(" + aggToString(s.functions) + ", fraction=" + s.fraction + ")\n" + toStringTree(s.child, intent + "     ")
    case s: DistinctSampleExec2 =>
      intent + "DIST(" + getHeaderOfOutput(s.groupingExpression.map(_.asInstanceOf[Attribute])) + aggToString(s.functions) + ", fraction=" + s.fraction + ")\n" + toStringTree(s.child, intent + "     ")
  }

  def aggToString(aggregateExpressions: Seq[AggregateExpression]) = aggregateExpressions.map(x => "" + x.aggregateFunction.children.map(p => getAttNameOfAtt(p.find(_.isInstanceOf[Attribute]).map(_.asInstanceOf[Attribute]).getOrElse(null))).mkString("") + "").mkString(",")


}
