package rules.physical

import operators.logical.UniversalSample
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BinaryExpression, Expression, NamedExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.execution.LogicalRDD

import scala.collection.Seq

abstract class SampleTransformationAbs extends Strategy {

  def pushUniversalSample(functions: Seq[AggregateExpression], confidence: Double, error: Double, seed: Long, left: LogicalPlan
                          , right: LogicalPlan, condition: Option[Expression]): (UniversalSample, UniversalSample) = {
    val joinKeyLeft = getEqualToFromExpression(condition.get)(0).left.find(_.isInstanceOf[AttributeReference]).get.asInstanceOf[AttributeReference]
    val joinKeyRight = getEqualToFromExpression(condition.get)(0).right.find(_.isInstanceOf[AttributeReference]).get.asInstanceOf[AttributeReference]
    if (right.output.find(x => x.toString().toLowerCase == joinKeyRight.toString().toLowerCase).isDefined)
      (UniversalSample(functions, confidence, error, seed, Seq(joinKeyRight), right)
        , UniversalSample(functions, confidence, error, seed, Seq(joinKeyLeft), left))
    else (UniversalSample(functions, confidence, error, seed, Seq(joinKeyLeft), right)
      , UniversalSample(functions, confidence, error, seed, Seq(joinKeyRight), left))
  }

  def getEqualToFromExpression(exp: Expression): Seq[org.apache.spark.sql.catalyst.expressions.EqualTo] = exp match {
    case a: UnaryExpression =>
      Seq()
    case b@org.apache.spark.sql.catalyst.expressions.EqualTo(left, right) =>
      if (left.find(_.isInstanceOf[AttributeReference]).isDefined && right.find(_.isInstanceOf[AttributeReference]).isDefined)
        Seq(b)
      else
        Seq()
    case b: BinaryExpression =>
      getEqualToFromExpression(b.left) ++ getEqualToFromExpression(b.right)
    case _ =>
      Seq()
  }

  def hasIncludeAtt(atts1: Seq[AttributeReference], atts2: Seq[AttributeReference]): Boolean = {
    var flag = true
    for (att <- atts2)
      if (atts1.filter(_.name.equalsIgnoreCase(att.name)).size == 0)
        flag = false
    flag
  }

  def getAttRefOfExps(exps: Seq[NamedExpression]): Seq[AttributeReference] =
    exps.map(_.find(_.isInstanceOf[AttributeReference]).map(_.asInstanceOf[AttributeReference]).get)

/*
  object ApproximatePhysicalAggregationSample {
    type ReturnType =
      (Double, Double, Long, Boolean, Seq[NamedExpression], Seq[NamedExpression], Seq[AggregateExpression], Seq[NamedExpression], LogicalPlan)

    def unapply(a: Any): Some[(Double, Double, Long, Boolean, Seq[NamedExpression], Seq[AggregateExpression], Seq[AggregateExpression], Seq[NamedExpression], LogicalPlan)] = a match {
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
    }
  }
*/

  def checkJoin(lp: LogicalPlan): Boolean = lp match {
    case j: Join =>
      true
    case l: LogicalRDD =>
      false
    case n =>
      n.children.foreach(child => if (checkJoin(child)) return true)
      false
  }

}
