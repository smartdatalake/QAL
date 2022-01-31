package rules.physical

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, Exists, Expression, ListQuery, SortOrder, SubqueryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec

case class PushFilterDown() extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    plan.transform {
      case FilterExec(condition, scan@RDDScanExec(output: Seq[Attribute], rdd: RDD[InternalRow], name: String, outputPartitioning: Partitioning, outputOrdering: Seq[SortOrder]))
        if (!splitConjunctivePredicates(condition).flatMap(definition.Paths.getAttRefFromExp).map(_.toString()).distinct.toSet.subsetOf(output.map(_.toString()).toSet)) =>
        var e: Expression = null
        splitConjunctivePredicates(condition).filter(x => definition.Paths.getAttRefFromExp(x).map(y => output.find(c => c.toString().equalsIgnoreCase(y.toString())).isDefined).reduce(_ && _)).foreach(x => if (e == null) e = x else e = And(e, x))

        FilterExec(e, scan)
      case FilterExec(condition, project@ProjectExec(fields, grandChild)) =>
        project.copy(child = FilterExec(condition, grandChild))
      case FilterExec(condition, sort@SortExec(sortOrder, global, grandChild, testSpillFrequency)) =>
        sort.copy(child = FilterExec(condition, grandChild))
      case FilterExec(condition, aggregate@ShuffleExchangeExec(newPartitioning, grandChild, coordinator)) =>
        aggregate.copy(child = FilterExec(condition, grandChild))
      case f@FilterExec(filterCondition, SortMergeJoinExec(leftKeys, rightKeys, joinType, joinCondition, left, right)) =>
        val (leftFilterConditions, rightFilterConditions, commonFilterCondition) =
          split(splitConjunctivePredicates(filterCondition), left, right)
        joinType match {
          case _: InnerLike =>
            // push down the single side `where` condition into respective sides
            val newLeft = leftFilterConditions.
              reduceLeftOption(And).map(FilterExec(_, left)).getOrElse(left)
            val newRight = rightFilterConditions.
              reduceLeftOption(And).map(FilterExec(_, right)).getOrElse(right)
            val (newJoinConditions, others) =
              commonFilterCondition.partition(canEvaluateWithinJoin)
            val newJoinCond = (newJoinConditions ++ joinCondition).reduceLeftOption(And)

            val join = SortMergeJoinExec(leftKeys, rightKeys, joinType, newJoinCond, newLeft, newRight)
            if (others.nonEmpty) {
              FilterExec(others.reduceLeft(And), join)
            } else {
              join
            }
          case RightOuter =>
            // push down the right side only `where` condition
            val newLeft = left
            val newRight = rightFilterConditions.
              reduceLeftOption(And).map(FilterExec(_, right)).getOrElse(right)
            val newJoinCond = joinCondition
            val newJoin = SortMergeJoinExec(leftKeys, rightKeys, RightOuter, newJoinCond, newLeft, newRight)
            (leftFilterConditions ++ commonFilterCondition).
              reduceLeftOption(And).map(FilterExec(_, newJoin)).getOrElse(newJoin)
          case LeftOuter | LeftExistence(_) =>
            // push down the left side only `where` condition
            val newLeft = leftFilterConditions.
              reduceLeftOption(And).map(FilterExec(_, left)).getOrElse(left)
            val newRight = right
            val newJoinCond = joinCondition
            val newJoin = SortMergeJoinExec(leftKeys, rightKeys, joinType, newJoinCond, newLeft, newRight)

            (rightFilterConditions ++ commonFilterCondition).
              reduceLeftOption(And).map(FilterExec(_, newJoin)).getOrElse(newJoin)
          case FullOuter => f // DO Nothing for Full Outer Join
          case NaturalJoin(_) => sys.error("Untransformed NaturalJoin node")
          case UsingJoin(_, _) => sys.error("Untransformed Using join node")
        }
      case j@SortMergeJoinExec(leftKeys, rightKeys, joinType, joinCondition, left, right) =>
        val (leftJoinConditions, rightJoinConditions, commonJoinCondition) =
          split(joinCondition.map(splitConjunctivePredicates).getOrElse(Nil), left, right)

        joinType match {
          case _: InnerLike | LeftSemi =>
            // push down the single side only join filter for both sides sub queries
            val newLeft = leftJoinConditions.
              reduceLeftOption(And).map(FilterExec(_, left)).getOrElse(left)
            val newRight = rightJoinConditions.
              reduceLeftOption(And).map(FilterExec(_, right)).getOrElse(right)
            val newJoinCond = commonJoinCondition.reduceLeftOption(And)

            SortMergeJoinExec(leftKeys, rightKeys, joinType, newJoinCond, newLeft, newRight)
          case RightOuter =>
            // push down the left side only join filter for left side sub query
            val newLeft = leftJoinConditions.
              reduceLeftOption(And).map(FilterExec(_, left)).getOrElse(left)
            val newRight = right
            val newJoinCond = (rightJoinConditions ++ commonJoinCondition).reduceLeftOption(And)

            SortMergeJoinExec(leftKeys, rightKeys, RightOuter, newJoinCond, newLeft, newRight)
          case LeftOuter | LeftAnti | ExistenceJoin(_) =>
            // push down the right side only join filter for right sub query
            val newLeft = left
            val newRight = rightJoinConditions.
              reduceLeftOption(And).map(FilterExec(_, right)).getOrElse(right)
            val newJoinCond = (leftJoinConditions ++ commonJoinCondition).reduceLeftOption(And)

            SortMergeJoinExec(leftKeys, rightKeys, joinType, newJoinCond, newLeft, newRight)
          case FullOuter => j
          case NaturalJoin(_) => sys.error("Untransformed NaturalJoin node")
          case UsingJoin(_, _) => sys.error("Untransformed Using join node")
        }
      case j: BinaryExecNode =>
        throw new Exception("Halt undefined join type to push its filter")
      case f@FilterExec(filterCondition, b: BinaryExecNode) =>
        throw new Exception("Halt undefined join type to push a filter below it")
      case FilterExec(condition, aggregate: HashAggregateExec) =>
        throw new Exception("Halt filter is above aggregate")
    //  case FilterExec(condition, a) if (a.isInstanceOf[operators.physical.SampleExec]) =>
    //    return plan
    //  case FilterExec(condition, a) if (!a.isInstanceOf[RDDScanExec]) =>
    //    throw new Exception("I cannot push down the filter")

    }


  }

  private def split(condition: Seq[Expression], left: SparkPlan, right: SparkPlan) = {
    val (pushDownCandidates, nonDeterministic) = condition.partition(_.deterministic)
    val (leftEvaluateCondition, rest) =
      pushDownCandidates.partition(_.references.subsetOf(left.outputSet))
    val (rightEvaluateCondition, commonCondition) =
      rest.partition(expr => expr.references.subsetOf(right.outputSet))

    (leftEvaluateCondition, rightEvaluateCondition, commonCondition ++ nonDeterministic)
  }

  def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  def splitCondition(exp: Expression): Seq[Expression] = exp match {
    case And(a, b) =>
      splitCondition(a) ++ splitCondition(b)
    case _ =>
      Seq(exp)
  }

  def canEvaluateWithinJoin(expr: Expression): Boolean = expr match {
    // Non-deterministic expressions are not allowed as join conditions.
    case e if !e.deterministic => false
    case _: ListQuery | _: Exists =>
      // A ListQuery defines the query which we want to search in an IN subquery expression.
      // Currently the only way to evaluate an IN subquery is to convert it to a
      // LeftSemi/LeftAnti/ExistenceJoin by `RewritePredicateSubquery` rule.
      // It cannot be evaluated as part of a Join operator.
      // An Exists shouldn't be push into a Join operator too.
      false
    case e: SubqueryExpression =>
      // non-correlated subquery will be replaced as literal
      e.children.isEmpty
    case a: AttributeReference => true
    case e: Unevaluable => false
    case e => e.children.forall(canEvaluateWithinJoin)
  }
}