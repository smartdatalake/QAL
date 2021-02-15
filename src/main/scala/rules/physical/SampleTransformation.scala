package rules.physical

import definition.Paths._
import operators.logical.{ApproximateAggregate, DistinctSample, Quantile, UniformSample, UniformSampleWithoutCI, UniversalSample, UniversalSampleWithoutKey}
import operators.physical.{DistinctSampleExec2, QuantileSampleExec, UniformSampleExec2, UniformSampleExec2WithoutCI, UniversalSampleExec2}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, EqualTo, EquivalentExpressions, Expression, NamedExpression, PythonUDF}
import org.apache.spark.sql.execution.aggregate.AggUtils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LeafNode, LogicalPlan, Project}

import scala.collection.{Seq, mutable}
import scala.io.Source

class p() {
  def apply(plan: SparkPlan): Seq[SparkPlan] = plan match {
    case s@UniformSampleExec2(functions: Seq[AggregateExpression], confidence: Double, error: Double,
    seed: Long,
    child: SparkPlan) =>
      Seq(child)
  }
}

object SampleTransformation extends Strategy {

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    /* case Quantile(quantileCol,quantilePart,confidence,error,seed,child)=>
      null
    case Binning(binningCol,binningPart,binningStart,binningEnd,confidence,error,seed,child)=>
      null*/
    case UniformSampleWithoutCI(seed, child) =>
      Seq(UniformSampleExec2WithoutCI(seed, planLater(child)))
    case ApproximatePhysicalAggregationSample(confidence, error, seed, hasJoin, groupingExpressions, functionsWithDistinct: Seq[AggregateExpression]
    , functionsWithoutDistinct: Seq[AggregateExpression], resultExpressions, child)
      if (!groupingExpressions.isEmpty) =>
      val sample =
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
      Seq(ScaleAggregateSampleExec(confidence, error, seed, 0.25, resultExpressions, sample(0)))
    case ApproximatePhysicalAggregationSample(confidence, error, seed, hasJoin, groupingExpressions, functionsWithDistinct: Seq[AggregateExpression]
    , functionsWithoutDistinct: Seq[AggregateExpression], resultExpressions, child)
      if (groupingExpressions.isEmpty && !hasJoin) =>
      val unifSample =
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
      //todo fix fraction
      Seq(ScaleAggregateSampleExec(confidence, error, seed, 0.25, resultExpressions, unifSample(0)))
    case ApproximatePhysicalAggregationSample(confidence, error, seed, hasJoin, groupingExpressions, functionsWithDistinct: Seq[AggregateExpression]
    , functionsWithoutDistinct: Seq[AggregateExpression], resultExpressions, child)
      if (groupingExpressions.isEmpty && hasJoin) =>
      val univSample =
        if (functionsWithDistinct.isEmpty) {
          AggUtils.planAggregateWithoutDistinct(
            groupingExpressions,
            functionsWithoutDistinct,
            resultExpressions,
            planLater(UniversalSampleWithoutKey(functionsWithoutDistinct, confidence, error, seed, child)))
        } else {
          AggUtils.planAggregateWithOneDistinct(
            groupingExpressions,
            functionsWithDistinct,
            functionsWithoutDistinct,
            resultExpressions,
            planLater(UniversalSampleWithoutKey(functionsWithDistinct, confidence, error, seed, child)))
        }
      //todo fix fraction
      Seq(ScaleAggregateSampleExec(confidence, error, seed, 0.25, resultExpressions, univSample(0)))
    //UNIVERSAL withOUT KEY TRANSFORMATION
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    case t@UniversalSampleWithoutKey(functions, confidence, error, seed, join
      @Join(left, right, joinType, condition)) =>
      val joinKeyLeft = getEqualToFromExpression(condition.get)(0).left.find(_.isInstanceOf[AttributeReference]).get.asInstanceOf[AttributeReference]
      val joinKeyRight = getEqualToFromExpression(condition.get)(0).right.find(_.isInstanceOf[AttributeReference]).get.asInstanceOf[AttributeReference]
      val (rightWithUniversalSample, leftWithUniversalSample) = if (right.output.find(x => x.toString().toLowerCase == joinKeyRight.toString().toLowerCase).isDefined)
        (UniversalSample(functions, confidence, error, seed, Seq(joinKeyRight), right)
          , UniversalSample(functions, confidence, error, seed, Seq(joinKeyLeft), left))
      else (UniversalSample(functions, confidence, error, seed, Seq(joinKeyLeft), right)
        , UniversalSample(functions, confidence, error, seed, Seq(joinKeyRight), left))
      Seq(planLater(Join(leftWithUniversalSample, rightWithUniversalSample, joinType, condition)))
    case t@UniversalSampleWithoutKey(functions, confidence, error, seed, project
      @Project(projectList: Seq[NamedExpression], join: Join)) =>
      Seq(/*UniformSampleExec2(functions, confidence, error, seed, planLater(project)),*/
        ProjectExec(projectList, planLater(UniversalSampleWithoutKey(functions, confidence, error, seed, join))))
    case t@UniversalSampleWithoutKey(functions, confidence, error, seed, project
      @Project(projectList: Seq[NamedExpression], projectChild: LogicalPlan)) =>
      Seq(ProjectExec(projectList, planLater(UniversalSampleWithoutKey(functions, confidence, error, seed, projectChild))))
    case t@UniversalSampleWithoutKey(functions, confidence, error, seed, filter
      @Filter(condition: Expression, filterChild: LogicalPlan)) =>
      Seq(FilterExec(condition, planLater(UniversalSampleWithoutKey(functions, confidence, error, seed, filterChild))))


    //UNIVERSAL with KEY TRANSFORMATION
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    case t@UniversalSample(functions, confidence, error, seed, joinKeys, join
      @Join(left, right, joinType, condition)) =>
      val joinKeyLeft = getEqualToFromExpression(condition.get)(0).left.find(_.isInstanceOf[AttributeReference]).get.asInstanceOf[AttributeReference]
      val joinKeyRight = getEqualToFromExpression(condition.get)(0).right.find(_.isInstanceOf[AttributeReference]).get.asInstanceOf[AttributeReference]
      val (rightWithUniversalSample, leftWithUniversalSample) = if (right.output.find(x => x.toString().toLowerCase == joinKeyRight.toString().toLowerCase).isDefined) (UniversalSample(functions, confidence, error, seed, Seq(joinKeyRight), right)
        , UniversalSample(functions, confidence, error, seed, Seq(joinKeyLeft), left))
      else (UniversalSample(functions, confidence, error, seed, Seq(joinKeyLeft), right)
        , UniversalSample(functions, confidence, error, seed, Seq(joinKeyRight), left))
      Seq(planLater(Join(leftWithUniversalSample, rightWithUniversalSample, joinType, condition))
        /*, UniversalSampleExec2(functions, confidence, error, seed, joinKeys, planLater(join))*/)
    case t@UniversalSample(functions, confidence, error, seed, joinKeys, project
      @Project(projectList: Seq[NamedExpression], logicalRDD@LogicalRDD(a, b, c, d, e))) =>
      Seq(UniversalSampleExec2(functions, confidence, error, seed, joinKeys, planLater(project)))
    case t@UniversalSample(functions, confidence, error, seed, joinKeys, project
      @Project(projectList: Seq[NamedExpression], projectChild: LogicalPlan)) =>
      Seq(ProjectExec(projectList, planLater(UniversalSample(functions, confidence, error, seed, joinKeys, projectChild)))
        /*,UniversalSampleExec2(functions, confidence, error, seed, joinKeys, planLater(project))*/)
    case t@UniversalSample(functions, confidence, error, seed, joinKeys, filter
      @Filter(condition: Expression, filterChild: LogicalPlan)) =>
      Seq(FilterExec(condition, planLater(UniversalSample(functions, confidence, error, seed, joinKeys, filterChild))))
    case t@UniversalSample(functions, confidence, error, seed, joinKeys, child
      @LogicalRDD(a, b, c, d, e)) =>
      Seq(UniversalSampleExec2(functions, confidence, error, seed, joinKeys, planLater(child)))

    //DISTINCT TRANSFORMATION
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    case t@DistinctSample(functions, confidence, error, seed, groupingExpressions, join
      @Join(left, right, joinType, condition)) =>
      val plan =
        if (hasIncludeAtt(left.output.map(_.asInstanceOf[AttributeReference]), getAttRefOfExps(groupingExpressions)))
          planLater(Join(DistinctSample(functions, confidence, error, seed, groupingExpressions, left), right, joinType, condition))
        else if (hasIncludeAtt(right.output.map(_.asInstanceOf[AttributeReference]), getAttRefOfExps(groupingExpressions)))
          planLater(Join(left, DistinctSample(functions, confidence, error, seed, groupingExpressions, right), joinType, condition))
        else {
          return Seq(DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, planLater(join)))
          //  throw new Exception("Unable to make distinct sample from two branches")
        }
      Seq(plan /*, DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, planLater(join))*/)
    case t@DistinctSample(functions, confidence, error, seed, groupingExpressions, project
      @Project(projectList: Seq[NamedExpression], logicalPlan@LogicalRDD(a, b, c, d, e))) =>
      Seq(DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, planLater(project)))
    case t@DistinctSample(functions, confidence, error, seed, groupingExpressions, project
      @Project(projectList: Seq[NamedExpression], projectChild: LogicalPlan)) =>
      Seq(ProjectExec(projectList, planLater(DistinctSample(functions, confidence, error, seed, groupingExpressions, projectChild)))
        /*,DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, planLater(project))*/)
    case t@DistinctSample(functions, confidence, error, seed, groupingExpressions, filter
      @Filter(condition: Expression, filterChild: LogicalPlan)) =>
      Seq(FilterExec(condition, planLater(DistinctSample(functions, confidence, error, seed, groupingExpressions, filterChild))))
    case t@DistinctSample(functions, confidence, error, seed, groupingExpressions, child
      @LogicalRDD(a, b, c, d, e)) =>
      Seq(DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, planLater(child)))

    //UNIFORM TRANSFORMATION
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    case t@UniformSample(functions, confidence, error, seed, join
      @Join(left, right, joinType, condition)) =>
      Seq(UniformSampleExec2(functions, confidence, error, seed, planLater(join)))
    case t@UniformSample(functions, confidence, error, seed, project
      @Project(projectList: Seq[NamedExpression], logicalRDD@LogicalRDD(a, b, c, d, e))) =>
      Seq(UniformSampleExec2(functions, confidence, error, seed, planLater(project)))
    case t@UniformSample(functions, confidence, error, seed, project
      @Project(projectList: Seq[NamedExpression], projectChild: LogicalPlan)) =>
      Seq(ProjectExec(projectList, planLater(UniformSample(functions, confidence, error, seed, projectChild)))
        /*,UniformSampleExec2(functions, confidence, error, seed, planLater(project))*/)
    case t@UniformSample(function, confidence, interval, seed, filter
      @Filter(condition: Expression, filterChild: LogicalPlan)) =>
      Seq(FilterExec(condition, planLater(UniformSample(function, confidence, interval, seed, filterChild))))
    case t@UniformSample(functions, confidence, error, seed, child
      @LogicalRDD(a, b, c, d, e)) =>
      Seq(UniformSampleExec2(functions, confidence, error, seed, planLater(child)))

    case q@Quantile(quantileColAtt, quantilePart, confidence, error, seed, child) =>
      if (checkJoin(child))
        Seq(QuantileSampleExec(quantileColAtt, quantilePart, planLater(UniversalSampleWithoutKey(null, confidence, error, seed, child))))
      else
        Seq(QuantileSampleExec(quantileColAtt, quantilePart, planLater(UniformSample(null, confidence, error, seed, child))))
    case _ => Nil
  }

  def hasIncludeAtt(atts1: Seq[AttributeReference], atts2: Seq[AttributeReference]): Boolean = {
    var flag = true
    for (att <- atts2)
      if (atts1.filter(_.name.equals(att.name)).size == 0)
        flag = false
    flag
  }

}

object ApproximatePhysicalAggregationSample {
  type ReturnType =
    (Double, Double, Long, Boolean, Seq[NamedExpression], Seq[Expression], Seq[Expression], Seq[NamedExpression], LogicalPlan)

  def unapply(a: Any): Option[ReturnType] = a match {
    case ApproximateAggregate(confidence, error, seed, groupingExpressions, aggExpressions, output, child) =>
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
      Some((confidence, error, seed, checkJoin(child),
        namedGroupingExpressions.map(_._2),
        functionsWithDistinct,
        functionsWithoutDistinct,
        rewrittenResultExpressions,
        child))

    case _ => None
  }

}


/*
*
* def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    /* case Quantile(quantileCol,quantilePart,confidence,error,seed,child)=>
      null
    case Binning(binningCol,binningPart,binningStart,binningEnd,confidence,error,seed,child)=>
      null*/
    case UniformSampleWithoutCI(seed, child) =>
      Seq(readOrCreateUniformSampleWithoutCIExec(seed, child))
    case ApproximatePhysicalAggregationSample(confidence, error, seed, hasJoin, groupingExpressions, functionsWithDistinct: Seq[AggregateExpression]
    , functionsWithoutDistinct: Seq[AggregateExpression], resultExpressions, child)
      if (!groupingExpressions.isEmpty) =>
      val sample =
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
      Seq(ScaleAggregateSampleExec(confidence, error, seed, 0.25, resultExpressions, sample(0)))
    case ApproximatePhysicalAggregationSample(confidence, error, seed, hasJoin, groupingExpressions, functionsWithDistinct: Seq[AggregateExpression]
    , functionsWithoutDistinct: Seq[AggregateExpression], resultExpressions, child)
      if (groupingExpressions.isEmpty) =>
      val unifSample =
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
      val univSample =
        if (functionsWithDistinct.isEmpty) {
          AggUtils.planAggregateWithoutDistinct(
            groupingExpressions,
            functionsWithoutDistinct,
            resultExpressions,
            planLater(UniversalSampleWithoutKey(functionsWithoutDistinct, confidence, error, seed, child)))
        } else {
          AggUtils.planAggregateWithOneDistinct(
            groupingExpressions,
            functionsWithDistinct,
            functionsWithoutDistinct,
            resultExpressions,
            planLater(UniversalSampleWithoutKey(functionsWithDistinct, confidence, error, seed, child)))
        }
      //todo fix fraction
      Seq(ScaleAggregateSampleExec(confidence, error, seed, 0.25, resultExpressions, unifSample(0)),
        ScaleAggregateSampleExec(confidence, error, seed, 0.25, resultExpressions, univSample(0)))

    //UNIVERSAL withOUT KEY TRANSFORMATION
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    case t@UniversalSampleWithoutKey(functions, confidence, error, seed, join
      @Join(left, right, joinType, condition)) =>
      val joinKeyLeft = condition.getOrElse(null).asInstanceOf[EqualTo].left.asInstanceOf[AttributeReference]
      val joinKeyRight = condition.getOrElse(null).asInstanceOf[EqualTo].right.asInstanceOf[AttributeReference]
      val rightWithUniversalSample = UniversalSample(functions, confidence, error, seed, Seq(joinKeyRight), right)
      val leftWithUniversalSample = UniversalSample(functions, confidence, error, seed, Seq(joinKeyLeft), left)
      Seq(planLater(Join(leftWithUniversalSample, rightWithUniversalSample, joinType, condition)))
    case t@UniversalSampleWithoutKey(functions, confidence, error, seed, project
      @Project(projectList: Seq[NamedExpression], projectChild: LogicalPlan)) =>
      Seq(ProjectExec(projectList, planLater(UniversalSampleWithoutKey(functions, confidence, error, seed, projectChild))))
    case t@UniversalSampleWithoutKey(functions, confidence, error, seed, filter
      @Filter(condition: Expression, filterChild: LogicalPlan)) =>
      Seq(FilterExec(condition, planLater(UniversalSampleWithoutKey(functions, confidence, error, seed, filterChild))))
    case t@UniversalSampleWithoutKey(functions, confidence, error, seed, child
      @LogicalRDD(a, b, c, d, e)) =>
      Seq(readOrCreateUniformSampleExec(functions, confidence, error, seed, child))


    //UNIVERSAL with KEY TRANSFORMATION
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    case t@UniversalSample(functions, confidence, error, seed, joinKeys, join
      @Join(left, right, joinType, condition)) =>
      val joinKeyLeft = condition.getOrElse(null).asInstanceOf[EqualTo].left.asInstanceOf[AttributeReference]
      val joinKeyRight = condition.getOrElse(null).asInstanceOf[EqualTo].right.asInstanceOf[AttributeReference]
      val rightWithUniversalSample = UniversalSample(functions, confidence, error, seed, Seq(joinKeyRight), right)
      val leftWithUniversalSample = UniversalSample(functions, confidence, error, seed, Seq(joinKeyLeft), left)
      Seq(planLater(Join(leftWithUniversalSample, rightWithUniversalSample, joinType, condition))
        , readOrCreateUniversalSampleExec(functions, confidence, error, seed, joinKeys, join))
    case t@UniversalSample(functions, confidence, error, seed, joinKeys, project
      @Project(projectList: Seq[NamedExpression], projectChild: LogicalPlan)) =>
      Seq(ProjectExec(projectList, planLater(UniversalSample(functions, confidence, error, seed, joinKeys, projectChild))),
        readOrCreateUniversalSampleExec(functions, confidence, error, seed, joinKeys, project))
    case t@UniversalSample(functions, confidence, error, seed, joinKeys, filter
      @Filter(condition: Expression, filterChild: LogicalPlan)) =>
      Seq(FilterExec(condition, planLater(UniversalSample(functions, confidence, error, seed, joinKeys, filterChild))))
    case t@UniversalSample(functions, confidence, error, seed, joinKeys, child
      @LogicalRDD(a, b, c, d, e)) =>
      Seq(readOrCreateUniversalSampleExec(functions, confidence, error, seed, joinKeys, child))

    //DISTINCT TRANSFORMATION
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    case t@DistinctSample(functions, confidence, error, seed, groupingExpressions, join
      @Join(left, right, joinType, condition)) =>
      val plan =
        if (hasIncludeAtt(left.output.map(_.asInstanceOf[AttributeReference]), groupingExpressions.map(_.asInstanceOf[AttributeReference]).toSeq))
          planLater(Join(DistinctSample(functions, confidence, error, seed, groupingExpressions, left), right, joinType, condition))
        else if (hasIncludeAtt(right.output.map(_.asInstanceOf[AttributeReference]), groupingExpressions.map(_.toAttribute.asInstanceOf[AttributeReference])))
          planLater(Join(left, DistinctSample(functions, confidence, error, seed, groupingExpressions, right), joinType, condition))
        else
          throw new Exception("Unable to make distinct sample from two branches")
      Seq(plan, readOrCreateDistinctSampleExec(functions, confidence, error, seed, groupingExpressions, join))
    case t@DistinctSample(functions, confidence, error, seed, groupingExpressions, project
      @Project(projectList: Seq[NamedExpression], projectChild: LogicalPlan)) =>
      Seq(ProjectExec(projectList, planLater(DistinctSample(functions, confidence, error, seed, groupingExpressions, projectChild))),
        readOrCreateDistinctSampleExec(functions, confidence, error, seed, groupingExpressions, project))
    case t@DistinctSample(functions, confidence, error, seed, groupingExpressions, filter
      @Filter(condition: Expression, filterChild: LogicalPlan)) =>
      Seq(FilterExec(condition, planLater(DistinctSample(functions, confidence, error, seed, groupingExpressions, filterChild))))
    case t@DistinctSample(functions, confidence, error, seed, groupingExpressions, child
      @LogicalRDD(a, b, c, d, e)) =>
      Seq(readOrCreateDistinctSampleExec(functions, confidence, error, seed, groupingExpressions, child))

    //UNIFORM TRANSFORMATION
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    case t@UniformSample(functions, confidence, error, seed, join
      @Join(left, right, joinType, condition)) =>
      Seq(readOrCreateUniformSampleExec(functions, confidence, error, seed, join))
    case t@UniformSample(functions, confidence, interval, seed, project
      @Project(projectList: Seq[NamedExpression], projectChild: LogicalPlan)) =>
      Seq(ProjectExec(projectList, planLater(UniformSample(functions, confidence, interval, seed, projectChild))),
        readOrCreateUniformSampleExec(functions, confidence, interval, seed, project))
    case t@UniformSample(function, confidence, interval, seed, filter
      @Filter(condition: Expression, filterChild: LogicalPlan)) =>
      Seq(FilterExec(condition, planLater(UniformSample(function, confidence, interval, seed, filterChild))))
    case t@UniformSample(function, confidence, interval, seed, child
      @LogicalRDD(a, b, c, d, e)) =>
      Seq(readOrCreateUniformSampleExec(function, confidence, interval, seed, child))

    case q@Quantile(quantileColAtt, quantilePart, confidence, error, seed, child) =>
      Seq(QuantileSampleExec(quantileColAtt, quantilePart, planLater(ApproximateUniversalJoin(confidence, error, seed
        , null, child))))
    //Seq(QuantileSampleExec(quantileColAtt, quantilePart, planLater( child)))
    case _ => Nil
  }
*
*
* */


/*    case ApproximatePhysicalAggregationSample(confidence, error, seed, hasJoin, groupingExpressions, functionsWithDistinct: Seq[AggregateExpression]
    , functionsWithoutDistinct: Seq[AggregateExpression], resultExpressions, child)
      if (hasJoin == true && !groupingExpressions.isEmpty) =>
      val sample =
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
      Seq(ScaleAggregateSampleExec(confidence, error, seed, 0.25, resultExpressions, sample(0)))
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
      withDistinctSample
    // Seq(ScaleAggregateSampleExec(confidence, error, seed, 0.25, resultExpressions, withDistinctSample(0)))
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
      withDistinctSample
    //   Seq(ScaleAggregateSampleExec(confidence, error, seed, 0.1, resultExpressions, withUniformSample(0)),
    //     ScaleAggregateSampleExec(confidence, error, seed, 0.1, resultExpressions, withDistinctSample(0)))

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
*/



