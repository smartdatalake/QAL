package rules.physical

import operators.logical._
import operators.physical._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, EquivalentExpressions, Expression, NamedExpression, PythonUDF}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.AggUtils

import scala.collection.Seq

object SampleTransformationMultiple extends SampleTransformationAbs {

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

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    // inject sampler based on the group by and joins
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    case ApproximatePhysicalAggregationSample(confidence, error, seed, hasJoin, groupingExpressions, functionsWithDistinct: Seq[AggregateExpression]
    , functionsWithoutDistinct: Seq[AggregateExpression], resultExpressions, child) =>
      if (!groupingExpressions.isEmpty) { // query with group by -> distinct sample
        val sample =
          if (functionsWithDistinct.isEmpty)
            AggUtils.planAggregateWithoutDistinct(
              groupingExpressions,
              functionsWithoutDistinct,
              resultExpressions,
              (planLater(DistinctSample(functionsWithoutDistinct, confidence, error, seed, groupingExpressions, child))/*,functionsWithoutDistinct.filter(_.find(_.isInstanceOf[Average]).isEmpty).flatMap(getAttRefFromExp)*/))
          else
            AggUtils.planAggregateWithOneDistinct(
              groupingExpressions,
              functionsWithDistinct,
              functionsWithoutDistinct,
              resultExpressions,
              (planLater(DistinctSample(functionsWithDistinct, confidence, error, seed, groupingExpressions, child))/*,functionsWithDistinct.filter(_.find(_.isInstanceOf[Average]).isEmpty).flatMap(getAttRefFromExp)*/))
        Seq(sample(0))
      }
      else if (groupingExpressions.isEmpty && !hasJoin) { // query without join and without group by -> unif
        val unifSample =
          if (functionsWithDistinct.isEmpty)
            AggUtils.planAggregateWithoutDistinct(
              groupingExpressions,
              functionsWithoutDistinct,
              resultExpressions,
              (planLater(UniformSample(functionsWithoutDistinct, confidence, error, seed, child))/*,functionsWithoutDistinct.filter(_.find(_.isInstanceOf[Average]).isEmpty).flatMap(getAttRefFromExp)*/))
          else
            AggUtils.planAggregateWithOneDistinct(
              groupingExpressions,
              functionsWithDistinct,
              functionsWithoutDistinct,
              resultExpressions,
              (planLater(UniformSample(functionsWithDistinct, confidence, error, seed, child))/*,functionsWithDistinct.filter(_.find(_.isInstanceOf[Average]).isEmpty).flatMap(getAttRefFromExp)*/))
        Seq(unifSample(0))
      }
      else if (groupingExpressions.isEmpty && hasJoin) { // query with join and without group by -> unif and univ
        val univSample =
          if (functionsWithDistinct.isEmpty)
            AggUtils.planAggregateWithoutDistinct(
              groupingExpressions,
              functionsWithoutDistinct,
              resultExpressions,
              (planLater(UniversalSampleWithoutKey(functionsWithoutDistinct, confidence, error, seed, child))/*,functionsWithoutDistinct.filter(_.find(_.isInstanceOf[Average]).isEmpty).flatMap(getAttRefFromExp)*/))
          else
            AggUtils.planAggregateWithOneDistinct(
              groupingExpressions,
              functionsWithDistinct,
              functionsWithoutDistinct,
              resultExpressions,
              (planLater(UniversalSampleWithoutKey(functionsWithDistinct, confidence, error, seed, child))/*,functionsWithDistinct.filter(_.find(_.isInstanceOf[Average]).isEmpty).flatMap(getAttRefFromExp)*/))
        Seq( univSample(0))
      }
      else
        throw new Exception("I cannot inject a proper sample")
    //UNIVERSAL without key transformation
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    case t@UniversalSampleWithoutKey(functions, confidence, error, seed, filter
      @Filter(conditionFilter: Expression, filterChild: LogicalPlan)) => filterChild match {
      case Join(left, right, joinType, condition) =>
        val (rightWithUniversalSample, leftWithUniversalSample) = pushUniversalSample(functions, confidence, error, seed, left, right, condition)
        Seq(FilterExec(conditionFilter, UniformSampleExec2(functions, confidence, error, seed
          , planLater(Join(leftWithUniversalSample, rightWithUniversalSample, joinType, condition))))
          , FilterExec(conditionFilter, planLater(Join(leftWithUniversalSample, rightWithUniversalSample, joinType, condition))))
      case p@Project(a, projectChild) =>
        Seq(FilterExec(conditionFilter, UniformSampleExec2(functions, confidence, error, seed,
          ProjectExec(a, planLater(UniversalSampleWithoutKey(functions, confidence, error, seed, projectChild)))))
          , FilterExec(conditionFilter, ProjectExec(a, planLater(UniversalSampleWithoutKey(functions, confidence, error, seed, projectChild)))))
      case _ =>
        throw new Exception("I cannot push universal sample below the filter")
    }
    case t@UniversalSampleWithoutKey(functions, confidence, error, seed, join
      @Join(left, right, joinType, condition)) =>
      val (rightWithUniversalSample, leftWithUniversalSample) = pushUniversalSample(functions, confidence, error, seed, left, right, condition)
      Seq(planLater(Join(leftWithUniversalSample, rightWithUniversalSample, joinType, condition)))
    case t@UniversalSampleWithoutKey(functions, confidence, error, seed, project
      @Project(projectList: Seq[NamedExpression], projectChild: LogicalPlan)) => projectChild match {
      case LogicalRDD(a, b, c, d, e) => Seq(UniformSampleExec2(functions, confidence, error, seed, planLater(project)))
      case _ => Seq(UniformSampleExec2(functions, confidence, error, seed, planLater(project))
        , ProjectExec(projectList, planLater(UniversalSampleWithoutKey(functions, confidence, error, seed, projectChild))))
    }

    //UNIVERSAL with KEY TRANSFORMATION
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    case t@UniversalSample(functions, confidence, error, seed, joinKeys, project
      @Project(projectList: Seq[NamedExpression], projectChild: LogicalPlan)) => projectChild match {
      case LogicalRDD(a, b, c, d, e) => Seq(UniversalSampleExec2(functions, confidence, error, seed, joinKeys, ProjectExec(projectList, planLater(projectChild))))
      case Join(left, right, joinType, condition) =>
        val (rightWithUniversalSample, leftWithUniversalSample) = pushUniversalSample(functions, confidence, error, seed, left, right, condition)
        Seq(UniversalSampleExec2(functions, confidence, error, seed, joinKeys, ProjectExec(projectList
          , planLater(Join(leftWithUniversalSample, rightWithUniversalSample, joinType, condition))))
          , ProjectExec(projectList, (planLater(Join(leftWithUniversalSample, rightWithUniversalSample, joinType, condition)))))
    }
    case t@UniversalSample(functions, confidence, error, seed, joinKeys, join
      @Join(left, right, joinType, condition)) =>
      val (rightWithUniversalSample, leftWithUniversalSample) = pushUniversalSample(functions, confidence, error, seed, left, right, condition)
      Seq(UniversalSampleExec2(functions, confidence, error, seed, joinKeys
        , (planLater(Join(leftWithUniversalSample, rightWithUniversalSample, joinType, condition))))
        , (planLater(Join(leftWithUniversalSample, rightWithUniversalSample, joinType, condition))))
    case t@UniversalSample(functions, confidence, error, seed, joinKeys, child
      @LogicalRDD(a, b, c, d, e)) =>
      Seq(UniversalSampleExec2(functions, confidence, error, seed, joinKeys, planLater(child)))

    //DISTINCT TRANSFORMATION
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    case DistinctSample(functions, confidence, error, seed, groupingExpressions,
    Filter(conditionFilter: Expression, filterChild: LogicalPlan)) => filterChild match {
      case Join(left, right, joinType, condition) =>
        if (hasIncludeAtt(left.output.map(_.asInstanceOf[AttributeReference]), getAttRefOfExps(groupingExpressions)))
          Seq(FilterExec(conditionFilter, DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions
            , (planLater(Join(DistinctSample(functions, confidence, error, seed, groupingExpressions, left), right, joinType, condition)))))
            , FilterExec(conditionFilter, (planLater(Join(DistinctSample(functions, confidence, error, seed, groupingExpressions, left), right, joinType, condition)))))
        else if (hasIncludeAtt(right.output.map(_.asInstanceOf[AttributeReference]), getAttRefOfExps(groupingExpressions)))
          Seq(FilterExec(conditionFilter, DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions
            , (planLater(Join(left, DistinctSample(functions, confidence, error, seed, groupingExpressions, right), joinType, condition)))))
            , FilterExec(conditionFilter, (planLater(Join(left, DistinctSample(functions, confidence, error, seed, groupingExpressions, right), joinType, condition)))))
        else
          Seq(FilterExec(conditionFilter, DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, planLater(filterChild))))
      case Project(projectList, Join(left, right, joinType, condition)) =>
        if (hasIncludeAtt(left.output.map(_.asInstanceOf[AttributeReference]), getAttRefOfExps(groupingExpressions)))
          Seq(FilterExec(conditionFilter, DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, ProjectExec(projectList
            , (planLater(Join(DistinctSample(functions, confidence, error, seed, groupingExpressions, left), right, joinType, condition))))))
            , FilterExec(conditionFilter, ProjectExec(projectList, (planLater(Join(DistinctSample(functions, confidence, error, seed, groupingExpressions, left), right, joinType, condition))))))
        else if (hasIncludeAtt(right.output.map(_.asInstanceOf[AttributeReference]), getAttRefOfExps(groupingExpressions)))
          Seq(FilterExec(conditionFilter, DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, ProjectExec(projectList
            , (planLater(Join(left, DistinctSample(functions, confidence, error, seed, groupingExpressions, right), joinType, condition))))))
            , FilterExec(conditionFilter, ProjectExec(projectList, (planLater(Join(left, DistinctSample(functions, confidence, error, seed, groupingExpressions, right), joinType, condition))))))
        else
          Seq(FilterExec(conditionFilter, DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, planLater(filterChild))))
      case Project(projectList, LogicalRDD(a, b, c, d, e)) =>
        Seq(FilterExec(conditionFilter, DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, planLater(filterChild))))
      case _ =>
        throw new Exception("I cannot push Distinct sample below the filter")
    }
    case DistinctSample(functions, confidence, error, seed, groupingExpressions, project
      @Project(projectList: Seq[NamedExpression], projectChild: LogicalPlan)) => projectChild match {
      case LogicalRDD(a, b, c, d, e) => Seq(DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, ProjectExec(projectList, planLater(projectChild))))
      case Join(left, right, joinType, condition) =>
        if (hasIncludeAtt(left.output.map(_.asInstanceOf[AttributeReference]), getAttRefOfExps(groupingExpressions)))
          Seq(DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, ProjectExec(projectList
            , ( planLater(Join(DistinctSample(functions, confidence, error, seed, groupingExpressions, left), right, joinType, condition)))))
            , ProjectExec(projectList, (planLater(Join(DistinctSample(functions, confidence, error, seed, groupingExpressions, left), right, joinType, condition)))))
        else if (hasIncludeAtt(right.output.map(_.asInstanceOf[AttributeReference]), getAttRefOfExps(groupingExpressions)))
          Seq(DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, ProjectExec(projectList
            , ( planLater(Join(left, DistinctSample(functions, confidence, error, seed, groupingExpressions, right), joinType, condition)))))
            , ProjectExec(projectList, (planLater(Join(left, DistinctSample(functions, confidence, error, seed, groupingExpressions, right), joinType, condition)))))
        else
          Seq(DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, planLater(project)))
    }
    case DistinctSample(functions, confidence, error, seed, groupingExpressions, join
      @Join(left, right, joinType, condition)) =>
      if (hasIncludeAtt(left.output.map(_.asInstanceOf[AttributeReference]), getAttRefOfExps(groupingExpressions)))
        Seq(DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, planLater(Join(DistinctSample(functions, confidence, error, seed, groupingExpressions, left), right, joinType, condition)))
          , (planLater(Join(DistinctSample(functions, confidence, error, seed, groupingExpressions, left), right, joinType, condition))))
      else if (hasIncludeAtt(right.output.map(_.asInstanceOf[AttributeReference]), getAttRefOfExps(groupingExpressions)))
        Seq(DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, planLater(Join(left, DistinctSample(functions, confidence, error, seed, groupingExpressions, right), joinType, condition)))
          , planLater(Join(left, DistinctSample(functions, confidence, error, seed, groupingExpressions, right), joinType, condition)))
      else
        Seq(DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, planLater(join)))
    case DistinctSample(functions, confidence, error, seed, groupingExpressions, child
      @LogicalRDD(a, b, c, d, e)) =>
      Seq(DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, planLater(child)))

    //UNIFORM TRANSFORMATION
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    case UniformSample(functions, confidence, error, seed, project
      @Project(projectList: Seq[NamedExpression], projectChild: LogicalPlan)) => projectChild match {
      case LogicalRDD(a, b, c, d, e) => Seq(UniformSampleExec2(functions, confidence, error, seed, ProjectExec(projectList, planLater(projectChild))))
      case Join(left, right, joinType, condition) =>
        Seq(UniformSampleExec2(functions, confidence, error, seed, ProjectExec(projectList, planLater(projectChild))))
      case _ => throw new Exception("I cannot push Uniform sample below project")
    }
    case UniformSample(functions, confidence, error, seed, filter
      @Filter(conditionFilter: Expression, filterChild: LogicalPlan)) =>
      Seq(FilterExec(conditionFilter, UniformSampleExec2(functions, confidence, error, seed, planLater(filterChild))))
    case t@UniformSample(functions, confidence, error, seed, child
      @LogicalRDD(a, b, c, d, e)) =>
      Seq(UniformSampleExec2(functions, confidence, error, seed, planLater(child)))
    case _ => Nil
  }

}