package rules.physical

import operators.logical.{ApproximateAggregate, ApproximateDistinctJoin, ApproximateUniversalJoin, DistinctSample, Quantile, UniformSample, UniformSampleWithoutCI, UniversalSample}
import operators.physical.{DistinctSampleExec2, DyadicRangeExec, QuantileSampleExec, QuantileSketchExec, UniformSampleExec2, UniformSampleExec2WithoutCI, UniversalSampleExec2}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, EqualTo, EquivalentExpressions, Expression, NamedExpression, PythonUDF}
import org.apache.spark.sql.execution.aggregate.AggUtils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LeafNode, LogicalPlan, Project}

import scala.collection.Seq
import scala.collection.mutable.ListBuffer

object SampleTransformation extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    /* case Quantile(quantileCol,quantilePart,confidence,error,seed,child)=>
      null
    case Binning(binningCol,binningPart,binningStart,binningEnd,confidence,error,seed,child)=>
      null*/
    case UniformSampleWithoutCI(seed, child) =>
      Seq(UniformSampleExec2WithoutCI(seed, planLater(child)))
    /*    case ApproximatePhysicalAggregationSample(confidence, error, seed, hasJoin, groupingExpressions, functionsWithDistinct: Seq[AggregateExpression]
    , functionsWithoutDistinct: Seq[AggregateExpression], resultExpressions, child)
      if (hasJoin == true && !groupingExpressions.isEmpty) =>
      val withDistinctSample =
        if (functionsWithDistinct.isEmpty) {
          AggUtils.planAggregateWithoutDistinct(
            groupingExpressions,
            functionsWithoutDistinct,
            resultExpressions,
            planLater(DistinctSample(functionsWithoutDistinct, confidence, error, seed, groupingExpressions, child)))
        } else {
          AggUtils.planAggregateWithOneDistinct(
            groupingExpressions,
            functionsWithDistinct,
            functionsWithoutDistinct,
            resultExpressions,
            planLater(DistinctSample(functionsWithDistinct, confidence, error, seed, groupingExpressions, child)))
        }
      //todo fix fraction
      Seq(ScaleAggregateSampleExec(confidence, error, seed, 0.15, resultExpressions, withDistinctSample(0)))*/
    case ApproximatePhysicalAggregationSample(confidence, error, seed, hasJoin, groupingExpressions, functionsWithDistinct: Seq[AggregateExpression]
    , functionsWithoutDistinct: Seq[AggregateExpression], resultExpressions, child)
      if (hasJoin == true && !groupingExpressions.isEmpty) =>
      val withNoSample =
        if (functionsWithDistinct.isEmpty) {
          AggUtils.planAggregateWithoutDistinct(
            groupingExpressions,
            functionsWithoutDistinct,
            resultExpressions,
            planLater(ApproximateDistinctJoin(confidence, error, seed, functionsWithoutDistinct, groupingExpressions, child)))
        } else {
          AggUtils.planAggregateWithOneDistinct(
            groupingExpressions,
            functionsWithDistinct,
            functionsWithoutDistinct,
            resultExpressions,
            planLater(ApproximateDistinctJoin(confidence, error, seed, functionsWithDistinct, groupingExpressions, child)))
        }
      //todo fix fraction
      Seq(ScaleAggregateSampleExec(confidence, error, seed, 0.25, resultExpressions, withNoSample(0)))
    case ApproximatePhysicalAggregationSample(confidence, error, seed, hasJoin, groupingExpressions, functionsWithDistinct: Seq[AggregateExpression]
    , functionsWithoutDistinct: Seq[AggregateExpression], resultExpressions, child)
      if (hasJoin == true && groupingExpressions.isEmpty) =>

      val withNoSample =
        if (functionsWithDistinct.isEmpty) {
          AggUtils.planAggregateWithoutDistinct(
            groupingExpressions,
            functionsWithoutDistinct,
            resultExpressions,
            planLater(ApproximateUniversalJoin(confidence, error, seed, functionsWithoutDistinct, child)))
        } else {
          AggUtils.planAggregateWithOneDistinct(
            groupingExpressions,
            functionsWithDistinct,
            functionsWithoutDistinct,
            resultExpressions,
            planLater(ApproximateUniversalJoin(confidence, error, seed, functionsWithDistinct, child)))
        }
      //todo fix fraction
      Seq(ScaleAggregateSampleExec(confidence, error, seed, 0.25, resultExpressions, withNoSample(0)))
    //todo distinct
    case ApproximatePhysicalAggregationSample(confidence, error, seed, hasJoin, groupingExpressions, functionsWithDistinct: Seq[AggregateExpression]
    , functionsWithoutDistinct: Seq[AggregateExpression], resultExpressions, child)
      if (hasJoin == false && !groupingExpressions.isEmpty) =>
      val withDistinctSample =
        if (functionsWithDistinct.isEmpty) {
          AggUtils.planAggregateWithoutDistinct(
            groupingExpressions,
            functionsWithoutDistinct,
            resultExpressions,
            planLater(DistinctSample(functionsWithoutDistinct, confidence, error, seed, groupingExpressions, child)))
        } else {
          AggUtils.planAggregateWithOneDistinct(
            groupingExpressions,
            functionsWithDistinct,
            functionsWithoutDistinct,
            resultExpressions,
            planLater(DistinctSample(functionsWithDistinct, confidence, error, seed, groupingExpressions, child)))
        }
      //todo fix fraction
      Seq(ScaleAggregateSampleExec(confidence, error, seed, 0.25, resultExpressions, withDistinctSample(0)))
    case ApproximatePhysicalAggregationSample(confidence, error, seed, hasJoin, groupingExpressions, functionsWithDistinct: Seq[AggregateExpression]
    , functionsWithoutDistinct: Seq[AggregateExpression], resultExpressions, child)
      if (hasJoin == false && groupingExpressions.isEmpty) =>
      val withUniformSample =
        if (functionsWithDistinct.isEmpty) {
          AggUtils.planAggregateWithoutDistinct(
            groupingExpressions,
            functionsWithoutDistinct,
            resultExpressions,
            planLater(UniformSample(functionsWithoutDistinct, confidence, error, seed, child)))
        } else {
          AggUtils.planAggregateWithOneDistinct(
            groupingExpressions,
            functionsWithDistinct,
            functionsWithoutDistinct,
            resultExpressions,
            planLater(UniformSample(functionsWithDistinct, confidence, error, seed, child)))
        }
      val withDistinctSample =
        if (functionsWithDistinct.isEmpty) {
          AggUtils.planAggregateWithoutDistinct(
            groupingExpressions,
            functionsWithoutDistinct,
            resultExpressions,
            planLater(DistinctSample(functionsWithoutDistinct, confidence, error, seed, groupingExpressions, child)))
        } else {
          AggUtils.planAggregateWithOneDistinct(
            groupingExpressions,
            functionsWithDistinct,
            functionsWithoutDistinct,
            resultExpressions,
            planLater(DistinctSample(functionsWithDistinct, confidence, error, seed, groupingExpressions, child)))
        }
      //todo fix fraction
      Seq(ScaleAggregateSampleExec(confidence, error, seed, 0.1, resultExpressions, withUniformSample(0)),
        ScaleAggregateSampleExec(confidence, error, seed, 0.1, resultExpressions, withDistinctSample(0)))

    case t@ApproximateDistinctJoin(confidence, error, seed, func, grouping, filter@Filter(condition: Expression, filterChild: LogicalPlan)) =>
      Seq(FilterExec(condition, planLater(ApproximateDistinctJoin(confidence, error, seed, func, grouping, filterChild))))
    case t@ApproximateDistinctJoin(confidence, error, seed, func, grouping, project@Project(projectList: Seq[NamedExpression], projectChild: LogicalPlan)) =>
      Seq(ProjectExec(projectList, planLater(ApproximateDistinctJoin(confidence, error, seed, func, grouping, projectChild))))
    case t@ApproximateDistinctJoin(confidence, error, seed, func, groupingExpression, join@Join(left, right, joinType, condition)) =>
      if (hasIncludeAtt(left.output.map(_.asInstanceOf[AttributeReference]), groupingExpression.map(_.asInstanceOf[AttributeReference]).toSeq))
        Seq(planLater(Join(DistinctSample(func, confidence, error, seed, groupingExpression, left), right, joinType, condition)))
      else if (hasIncludeAtt(right.output.map(_.asInstanceOf[AttributeReference]), groupingExpression.map(_.toAttribute.asInstanceOf[AttributeReference])))
        Seq(planLater(Join(left, DistinctSample(func, confidence, error, seed, groupingExpression, right), joinType, condition)))
      else
        throw new Exception("Unable to make distinct sample from two branches")
    case t@ApproximateUniversalJoin(confidence, error, seed, func, filter@Filter(condition: Expression, filterChild: LogicalPlan)) =>
      Seq(FilterExec(condition, planLater(ApproximateUniversalJoin(confidence, error, seed, func, filterChild))))
    case t@ApproximateUniversalJoin(confidence, error, seed, func, project@Project(projectList: Seq[NamedExpression], projectChild: LogicalPlan)) =>
      Seq(ProjectExec(projectList, planLater(ApproximateUniversalJoin(confidence, error, seed, func, projectChild))))
    case t@ApproximateUniversalJoin(confidence, error, seed, func, join@Join(left, right, joinType, condition)) =>
      val joinKeyLeft = condition.getOrElse(null).asInstanceOf[EqualTo].left.asInstanceOf[AttributeReference]
      val joinKeyRight = condition.getOrElse(null).asInstanceOf[EqualTo].right.asInstanceOf[AttributeReference]
      val rightWithUniversalSample = UniversalSample(func, confidence, error, seed, Seq(joinKeyRight), right)
      val leftWithUniversalSample = UniversalSample(func, confidence, error, seed, Seq(joinKeyLeft), left)
      Seq(planLater(Join(leftWithUniversalSample, rightWithUniversalSample, joinType, condition)))
    case t@UniversalSample(functions, confidence, error, seed, joinKeys, project
      @Project(projectList: Seq[NamedExpression], projectChild: LogicalPlan)) =>
      val plans = new ListBuffer[SparkPlan]
      //plans+=UniversalSampleExec2(functions,confidence,error,seed,joinKeys,planLater(project))
      plans += planLater(Project(projectList, UniversalSample(functions, confidence, error, seed, joinKeys, projectChild)))
      plans
    case t@UniversalSample(functions, confidence, error, seed, joinKeys, filter
      @Filter(condition: Expression, filterChild: LogicalPlan)) =>
      val plans = new ListBuffer[SparkPlan]
      //plans+=UniversalSampleExec2(functions,confidence,error,seed,joinKeys,planLater(filter))
      plans += planLater(Filter(condition, UniversalSample(functions, confidence, error, seed, joinKeys, filterChild)))
      plans
    case t@UniversalSample(functions, confidence, error, seed, joinKeys, child) =>
      Seq(UniversalSampleExec2(functions, confidence, error, seed, joinKeys, planLater(child)))

    case t@DistinctSample(functions, confidence, error, seed, groupingExpressions, project
      @Project(projectList: Seq[NamedExpression], projectChild: LogicalPlan)) =>
      val plans = new ListBuffer[SparkPlan]
      //plans+=DistinctSampleExec2(functions,confidence,error,seed,groupingExpressions,planLater(project))
      plans += planLater(Project(projectList, DistinctSample(functions, confidence, error, seed, groupingExpressions, projectChild)))
      plans
    case t@DistinctSample(functions, confidence, error, seed, groupingExpressions, filter
      @Filter(condition: Expression, filterChild: LogicalPlan)) =>
      val plans = new ListBuffer[SparkPlan]
      //plans+=DistinctSampleExec2(functions,confidence,error,seed,groupingExpressions,planLater(filter))
      plans += planLater(Filter(condition, DistinctSample(functions, confidence, error, seed, groupingExpressions, filterChild)))
      plans
    case t@DistinctSample(functions, confidence, error, seed, groupingExpressions, child) =>
      Seq(DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, planLater(child)))
    case t@UniformSample(function, confidence, interval, seed, project@Project(projectList: Seq[NamedExpression], projectChild: LogicalPlan)) =>
      val plans = new ListBuffer[SparkPlan]
      //plans+=UniformSampleExec2(0,confidence,interval,seed,planLater(project))
      plans += ProjectExec(projectList, planLater(UniformSample(function, confidence, interval, seed, projectChild)))
      plans
    case t@UniformSample(function, confidence, interval, seed, filter@Filter(condition: Expression, filterChild: LogicalPlan)) =>
      val plans = new ListBuffer[SparkPlan]
      //plans+=UniformSampleExec2(0,confidence,interval,seed,planLater(filter))
      plans += FilterExec(condition, planLater(UniformSample(function, confidence, interval, seed, filterChild)))
      plans
    case t@UniformSample(function, confidence, interval, seed, child) =>
      Seq(UniformSampleExec2(function, confidence, interval, seed, planLater(child)))
    case q@Quantile(quantileColAtt, quantilePart, confidence, error, seed, child) =>
      Seq(QuantileSampleExec(quantileColAtt, quantilePart, planLater(ApproximateUniversalJoin(confidence, error, seed
        , null, child))))
      //Seq(QuantileSampleExec(quantileColAtt, quantilePart, planLater( child)))
    case _ => Nil
  }

  def hasIncludeAtt(atts1: Seq[AttributeReference], atts2: Seq[AttributeReference]): Boolean = {
    var flag = true
    for (att <- atts2)
      if (atts1.filter(_.name.equals(att.name)).size==0)
        flag = false
    flag
  }
}
object ApproximatePhysicalAggregationSample {
  type ReturnType =
    (Double, Double,Long,Boolean, Seq[NamedExpression],Seq[Expression], Seq[Expression], Seq[NamedExpression], LogicalPlan)

  def unapply(a: Any): Option[ReturnType] = a match {
    case ApproximateAggregate(confidence, error,seed, groupingExpressions, aggExpressions, output,child) =>
      // A single aggregate expression might appear multiple times in resultExpressions.
      // In order to avoid evaluating an individual aggregate function multiple times, we'll
      // build a set of semantically distinct aggregate expressions and re-write expressions so
      // that they reference the single copy of the aggregate function which actually gets computed.
      // Non-deterministic aggregate expressions are not deduplicated.
      val equivalentAggregateExpressions = new EquivalentExpressions
      val aggregateExpressions = aggExpressions.flatMap { expr =>
        expr.collect {
          // addExpr() always returns false for non-deterministic expressions and do not add them.
          case agg: AggregateExpression
            if !equivalentAggregateExpressions.addExpr(agg) => agg
          case udf: PythonUDF
            if PythonUDF.isGroupedAggPandasUDF(udf) &&
              !equivalentAggregateExpressions.addExpr(udf) => udf
        }
      }

      val namedGroupingExpressions = groupingExpressions.map {
        case ne: NamedExpression => ne -> ne
        // If the expression is not a NamedExpressions, we add an alias.
        // So, when we generate the result of the operator, the Aggregate Operator
        // can directly get the Seq of attributes representing the grouping expressions.
        case other =>
          val withAlias = Alias(other, other.toString)()
          other -> withAlias
      }
      val groupExpressionMap = namedGroupingExpressions.toMap

      // The original `resultExpressions` are a set of expressions which may reference
      // aggregate expressions, grouping column values, and constants. When aggregate operator
      // emits output rows, we will use `resultExpressions` to generate an output projection
      // which takes the grouping columns and final aggregate result buffer as input.
      // Thus, we must re-write the result expressions so that their attributes match up with
      // the attributes of the final result projection's input row:
      val rewrittenResultExpressions = aggExpressions.map { expr =>
        expr.transformDown {
          case ae: AggregateExpression =>
            // The final aggregation buffer's attributes will be `finalAggregationAttributes`,
            // so replace each aggregate expression by its corresponding attribute in the set:
            equivalentAggregateExpressions.getEquivalentExprs(ae).headOption
              .getOrElse(ae).asInstanceOf[AggregateExpression].resultAttribute
          // Similar to AggregateExpression
          case ue: PythonUDF if PythonUDF.isGroupedAggPandasUDF(ue) =>
            equivalentAggregateExpressions.getEquivalentExprs(ue).headOption
              .getOrElse(ue).asInstanceOf[PythonUDF].resultAttribute
          case expression =>
            // Since we're using `namedGroupingAttributes` to extract the grouping key
            // columns, we need to replace grouping key expressions with their corresponding
            // attributes. We do not rely on the equality check at here since attributes may
            // differ cosmetically. Instead, we use semanticEquals.
            groupExpressionMap.collectFirst {
              case (expr, ne) if expr semanticEquals expression => ne.toAttribute
            }.getOrElse(expression)
        }.asInstanceOf[NamedExpression]
      }
      val (functionsWithDistinct, functionsWithoutDistinct) =
        aggregateExpressions.map(_.asInstanceOf[AggregateExpression]).partition(_.isDistinct)
      if (functionsWithDistinct.map(_.aggregateFunction.children.toSet).distinct.length > 1) {
        // This is a sanity check. We should not reach here when we have multiple distinct
        // column sets. Our `RewriteDistinctAggregates` should take care this case.
        sys.error("You hit a query analyzer bug. Please report your query to " +
          "Spark user mailing list.")
      }
      var plan = child
      var logicalRDD:LogicalRDD=if (child.isInstanceOf[LogicalRDD]) child.asInstanceOf[LogicalRDD] else null
      var hasJoin=false
      while (!plan.isInstanceOf[Join] && !plan.isInstanceOf[LeafNode]) {
        if (plan.isInstanceOf[LogicalRDD])
          logicalRDD = plan.asInstanceOf[LogicalRDD]
        plan = plan.children(0)
      }
      if (plan.isInstanceOf[LogicalRDD])
        logicalRDD = plan.asInstanceOf[LogicalRDD]
      if(plan.isInstanceOf[Join])
        hasJoin=true
      Some((confidence, error, seed,hasJoin,
        namedGroupingExpressions.map(_._2),
        functionsWithDistinct,
        functionsWithoutDistinct,
        rewrittenResultExpressions,
        child))

    case _ => None
  }


}



