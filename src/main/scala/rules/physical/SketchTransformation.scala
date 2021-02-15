package rules.physical

import operators.logical.{ApproximateAggregate, Binning, BinningWithoutMinMax, Quantile}
import operators.physical.{BinningSketchExec, BinningWithoutMaxMinSketchExec, CountMinSketchExec, DyadicRangeExec, ExtAggregateExec, QuantileSketchExec}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, AttributeReference, BinaryComparison, BinaryOperator, EqualNullSafe, EqualTo, EquivalentExpressions, Expression, GreaterThan, GreaterThanOrEqual, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, NamedExpression, Or, PredicateHelper, PythonUDF, UnaryExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalAggregation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.execution.aggregate.AggUtils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.types.{IntegerType, NumericType, StringType}
import org.apache.spark.sql.{Strategy, execution}

import scala.collection.Seq
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

object SketchTransformation extends Strategy with PredicateHelper {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    /*    case cms@CountMinSketch(delta, eps, seed, child, outputExpression, condition) =>
      Seq(CountMinSketchExec(delta, eps, seed, outputExpression, condition, planLater(child)))*/
    case _ => Nil
  }

}

object SketchPhysicalTransformation extends Strategy {
  def getConditions(expression: Expression): String = {
    val t = expression
    while (t != null) {
      if (t.isInstanceOf[AttributeReference])
        if (t.dataType.isInstanceOf[NumericType] || t.dataType.isInstanceOf[StringType])
          return t.asInstanceOf[AttributeReference].name
        else return ""
      else if (t.isInstanceOf[UnaryExpression]) {
        return getValidParam(t.children(0))
      }
      else if (t.isInstanceOf[Literal]) {
        return null
      }
      else {
        val left = getValidParam(t.asInstanceOf[BinaryOperator].left)
        val right = getValidParam(t.asInstanceOf[BinaryOperator].right)
        if (left == "" || right == "")
          return ""
        if (left == null)
          return right
        if (right == null)
          return left
        if ((left == right && left != ""))
          return left
        return ""
      }
    }
    ""
  }

  def contractHyperRectangles(leftt: Seq[Seq[And]], rightt: Seq[Seq[And]]): Seq[Seq[And]] = {
    val out = new ListBuffer[Seq[And]]
    val left = leftt.sortBy(_ (0).left.asInstanceOf[AttributeReference].name)
    val right = rightt.sortBy(_ (0).left.asInstanceOf[AttributeReference].name)
    if (right.size == 0) return left
    if (left.size == 0) return right
    for (hyperRecLeft <- left)
      for (hyperRecRight <- right) {
        val con = new ListBuffer[And]
        breakable {
          for (edgehyperRecLeft <- hyperRecLeft) {
            var flag = true
            for (edgeHyperRecRight <- hyperRecRight) {
              if (edgehyperRecLeft.left.children(0).asInstanceOf[AttributeReference].name == edgeHyperRecRight.left.children(0).asInstanceOf[AttributeReference].name) {
                flag = false
                val x_1 = getRangeIntValue(edgehyperRecLeft.left.asInstanceOf[BinaryComparison])
                val x_r = getRangeIntValue(edgehyperRecLeft.right.asInstanceOf[BinaryComparison])
                val y_l = getRangeIntValue(edgeHyperRecRight.left.asInstanceOf[BinaryComparison])
                val y_r = getRangeIntValue(edgeHyperRecRight.right.asInstanceOf[BinaryComparison])
                if (x_r < y_l || x_r < y_l)
                  break
                if (x_1 > y_l) {
                  if (x_r < y_r)
                    con.+=(And(GreaterThanOrEqual(getAttributeReference(edgehyperRecLeft.left.asInstanceOf[BinaryComparison]), Literal(x_1, edgeHyperRecRight.left.children(0).dataType))
                      , LessThanOrEqual(getAttributeReference(edgehyperRecLeft.right.asInstanceOf[BinaryComparison]), Literal(x_r, edgeHyperRecRight.left.children(0).dataType))))
                  else
                    And(GreaterThanOrEqual(getAttributeReference(edgehyperRecLeft.left.asInstanceOf[BinaryComparison]), Literal(x_1, edgeHyperRecRight.left.children(0).dataType))
                      , LessThanOrEqual(getAttributeReference(edgeHyperRecRight.right.asInstanceOf[BinaryComparison]), Literal(y_r, edgeHyperRecRight.left.children(0).dataType)))
                } else {
                  if (x_r < y_r)
                    con.+=(And(GreaterThanOrEqual(getAttributeReference(edgeHyperRecRight.left.asInstanceOf[BinaryComparison]), Literal(y_l, edgeHyperRecRight.left.children(0).dataType))
                      , LessThanOrEqual(getAttributeReference(edgehyperRecLeft.right.asInstanceOf[BinaryComparison]), Literal(x_r, edgeHyperRecRight.left.children(0).dataType))))
                  else
                    con.+=(And(GreaterThanOrEqual(getAttributeReference(edgeHyperRecRight.left.asInstanceOf[BinaryComparison]), Literal(y_l, edgeHyperRecRight.left.children(0).dataType))
                      , LessThanOrEqual(getAttributeReference(edgeHyperRecRight.right.asInstanceOf[BinaryComparison]), Literal(y_r, edgeHyperRecRight.left.children(0).dataType))))
                }
              }
            }
            if (flag)
              con += edgehyperRecLeft
          }
          for (edgeHyperRecRight <- hyperRecRight) {
            var flag = true
            for (edgehyperRecLeft <- hyperRecLeft)
              if (edgehyperRecLeft.left.children(0).asInstanceOf[AttributeReference].name == edgeHyperRecRight.left.children(0).asInstanceOf[AttributeReference].name)
                flag = false
            if (flag)
              con += (edgeHyperRecRight)
          }
          out += (con)
        }
      }
    out
  }

  def combineHyperRectangles(left: Seq[Seq[And]], right: Seq[Seq[And]]): Seq[Seq[And]] = {
    //todo fix overlapping union
    val out = new ListBuffer[Seq[And]]
    for (leftRec <- left) {
      if (contractHyperRectangles(Seq(leftRec), right).size == 0) {
        out ++= (left)
        out ++= (right)
      }
    }
    out
  }

  def combineHyperRectangles(expression: Expression): Seq[Seq[And]] = {
    if (expression != null) {
      if (expression.isInstanceOf[And]) {
        if (expression.asInstanceOf[And].left.isInstanceOf[BinaryComparison] && expression.asInstanceOf[And].right.isInstanceOf[BinaryComparison])
          return contractHyperRectangles(Seq(Seq(convertBCToAnd(expression.asInstanceOf[And].left.asInstanceOf[BinaryComparison])))
            , Seq(Seq(convertBCToAnd(expression.asInstanceOf[And].right.asInstanceOf[BinaryComparison]))))
        if (expression.asInstanceOf[And].left.isInstanceOf[IsNotNull] && expression.asInstanceOf[And].right.isInstanceOf[BinaryComparison])
          return Seq(Seq(convertBCToAnd(expression.asInstanceOf[And].right.asInstanceOf[BinaryComparison])))
        if (expression.asInstanceOf[And].right.isInstanceOf[IsNotNull] && expression.asInstanceOf[And].left.isInstanceOf[BinaryComparison])
          return Seq(Seq(convertBCToAnd(expression.asInstanceOf[And].left.asInstanceOf[BinaryComparison])))
        if (expression.asInstanceOf[And].left.isInstanceOf[IsNotNull] && expression.asInstanceOf[And].right.isInstanceOf[BinaryOperator])
          return combineHyperRectangles(expression.asInstanceOf[And].right)
        if (expression.asInstanceOf[And].right.isInstanceOf[IsNotNull] && expression.asInstanceOf[And].left.isInstanceOf[BinaryOperator])
          return combineHyperRectangles(expression.asInstanceOf[And].left)
        if (expression.asInstanceOf[And].left.isInstanceOf[BinaryComparison] && expression.asInstanceOf[And].right.isInstanceOf[BinaryOperator]) {
          return contractHyperRectangles(Seq(Seq(convertBCToAnd(expression.asInstanceOf[And].left.asInstanceOf[BinaryComparison])))
            , combineHyperRectangles(expression.asInstanceOf[And].right))
          /*val left =
          var out: Seq[And] = Seq()
          for (range <- right)
            out = out.++:(Seq(contractHyperRectangles(left, range)))
          return out*/
        }
        if (expression.asInstanceOf[And].right.isInstanceOf[BinaryComparison] && expression.asInstanceOf[And].left.isInstanceOf[BinaryOperator]) {
          return contractHyperRectangles(Seq(Seq(convertBCToAnd(expression.asInstanceOf[And].right.asInstanceOf[BinaryComparison])))
            , combineHyperRectangles(expression.asInstanceOf[And].left))
          /*val left = combineRanges(expression.asInstanceOf[And].left.asInstanceOf[BinaryOperator])
          val right = convertBCToAnd(expression.asInstanceOf[And].right.asInstanceOf[BinaryComparison])
          var out: Seq[And] = Seq()
          for (range <- left)
            out = out.++:(Seq(contractHyperRectangles(range, right)))
          return out.asInstanceOf[Seq[And]]*/
        }
      }
      else if (expression.isInstanceOf[Or]) {
        if (expression.asInstanceOf[Or].left.isInstanceOf[BinaryComparison]
          && expression.asInstanceOf[Or].right.isInstanceOf[BinaryComparison])
          return combineHyperRectangles(Seq(Seq(convertBCToAnd(expression.asInstanceOf[Or].left.asInstanceOf[BinaryComparison])))
            , Seq(Seq(convertBCToAnd(expression.asInstanceOf[Or].right.asInstanceOf[BinaryComparison]))))
        if (expression.asInstanceOf[Or].left.isInstanceOf[BinaryComparison]
          && expression.asInstanceOf[Or].right.isInstanceOf[BinaryOperator])
          return combineHyperRectangles(Seq(Seq(convertBCToAnd(expression.asInstanceOf[Or].left.asInstanceOf[BinaryComparison])))
            , combineHyperRectangles(expression.asInstanceOf[Or].right))
        if (expression.asInstanceOf[Or].left.isInstanceOf[BinaryOperator]
          && expression.asInstanceOf[Or].right.isInstanceOf[BinaryComparison])
          return combineHyperRectangles(Seq(Seq(convertBCToAnd(expression.asInstanceOf[Or].right.asInstanceOf[BinaryComparison])))
            , combineHyperRectangles(expression.asInstanceOf[Or].left))
        if (expression.asInstanceOf[Or].left.isInstanceOf[BinaryOperator]
          && expression.asInstanceOf[Or].right.isInstanceOf[BinaryOperator])
          return combineHyperRectangles(combineHyperRectangles(expression.asInstanceOf[Or].left)
            , combineHyperRectangles(expression.asInstanceOf[Or].right))
      }
      Seq()
    }
    Seq()
  }

  def combineRanges(expression: Expression): Seq[And] = {
    if (expression != null) {
      if (expression.isInstanceOf[And]) {
        /*if (expression.asInstanceOf[And].left.isInstanceOf[BinaryComparison] && expression.asInstanceOf[And].right.isInstanceOf[BinaryComparison])
          return Seq(contractHyperRectangles(Seq(convertBCToAnd(expression.asInstanceOf[And].left.asInstanceOf[BinaryComparison]))
            , Seq(convertBCToAnd(expression.asInstanceOf[And].right.asInstanceOf[BinaryComparison]))))*/
        if (expression.asInstanceOf[And].left.isInstanceOf[IsNotNull] && expression.asInstanceOf[And].right.isInstanceOf[BinaryComparison])
          return Seq(convertBCToAnd(expression.asInstanceOf[And].right.asInstanceOf[BinaryComparison]))
        if (expression.asInstanceOf[And].right.isInstanceOf[IsNotNull] && expression.asInstanceOf[And].left.isInstanceOf[BinaryComparison])
          return Seq(convertBCToAnd(expression.asInstanceOf[And].left.asInstanceOf[BinaryComparison]))
        if (expression.asInstanceOf[And].left.isInstanceOf[IsNotNull] && expression.asInstanceOf[And].right.isInstanceOf[BinaryOperator])
          return combineRanges(expression.asInstanceOf[And].right)
        if (expression.asInstanceOf[And].right.isInstanceOf[IsNotNull] && expression.asInstanceOf[And].left.isInstanceOf[BinaryOperator])
          return combineRanges(expression.asInstanceOf[And].left)
        if (expression.asInstanceOf[And].left.isInstanceOf[BinaryComparison] && expression.asInstanceOf[And].right.isInstanceOf[BinaryOperator]) {
          val left = convertBCToAnd(expression.asInstanceOf[And].left.asInstanceOf[BinaryComparison])
          val right = combineRanges(expression.asInstanceOf[And].right)
          var out: Seq[And] = Seq()
          // for (range <- right)
          //   out = out.++:(Seq(contractHyperRectangles(left, range)))
          return out
        }
        if (expression.asInstanceOf[And].right.isInstanceOf[BinaryComparison] && expression.asInstanceOf[And].left.isInstanceOf[BinaryOperator]) {
          val left = combineRanges(expression.asInstanceOf[And].left.asInstanceOf[BinaryOperator])
          val right = convertBCToAnd(expression.asInstanceOf[And].right.asInstanceOf[BinaryComparison])
          var out: Seq[And] = Seq()
          // for (range <- left)
          //   out = out.++:(Seq(contractHyperRectangles(range, right)))
          return out.asInstanceOf[Seq[And]]
        }
      }
      else if (expression.isInstanceOf[Or]) {
        if (expression.asInstanceOf[Or].left.isInstanceOf[BinaryComparison]
          && expression.asInstanceOf[Or].right.isInstanceOf[BinaryComparison])
          return unionRangeWithRange(convertBCToAnd(expression.asInstanceOf[Or].left.asInstanceOf[BinaryComparison])
            , convertBCToAnd(expression.asInstanceOf[Or].right.asInstanceOf[BinaryComparison]))
        if (expression.asInstanceOf[Or].left.isInstanceOf[BinaryComparison]
          && expression.asInstanceOf[Or].right.isInstanceOf[BinaryOperator])
          return unionRangeWithRanges(convertBCToAnd(expression.asInstanceOf[Or].left.asInstanceOf[BinaryComparison])
            , combineRanges(expression.asInstanceOf[Or].right))
        if (expression.asInstanceOf[Or].left.isInstanceOf[BinaryOperator]
          && expression.asInstanceOf[Or].right.isInstanceOf[BinaryComparison])
          return unionRangeWithRanges(convertBCToAnd(expression.asInstanceOf[Or].right.asInstanceOf[BinaryComparison])
            , combineRanges(expression.asInstanceOf[Or].left))
        if (expression.asInstanceOf[Or].left.isInstanceOf[BinaryOperator]
          && expression.asInstanceOf[Or].right.isInstanceOf[BinaryOperator])
          return unionRangesWithRanges(combineRanges(expression.asInstanceOf[Or].left)
            , combineRanges(expression.asInstanceOf[Or].right))
      }
      Seq()
    }
    Seq()
  }

  def getDimension(hyperRects: Seq[Seq[And]]): Int = {
    1
  }

  def getPossibleCMSPlan(): TraversableOnce[SparkPlan] = ???

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case q@Quantile(quantileCol, quantilePart, confidence, error, seed, child: LogicalRDD) =>
      Seq(QuantileSketchExec(quantilePart, q.output, DyadicRangeExec(null, confidence, error, seed, null, null, quantileCol, child)))
    case b@Binning(binningCol, binningPart, binningStart, binningEnd, confidence, error, seed, child: LogicalRDD) =>
      Seq(BinningSketchExec(binningPart, binningStart, binningEnd, b.output, DyadicRangeExec(null, confidence, error, seed, null, null, binningCol, child)))
    case b@BinningWithoutMinMax(binningCol, binningPart, confidence, error, seed, child: LogicalRDD) =>
      Seq(BinningWithoutMaxMinSketchExec(binningPart, b.output, DyadicRangeExec(null, confidence, error, seed, null, null, binningCol, child)))
    case ApproximatePhysicalAggregation(confidence, error, seed, hasJoin, groupingExpressions
    , functionsWithDistinct: Seq[AggregateExpression], functionsWithoutDistinct: Seq[AggregateExpression]
    , resultExpressions, logicalRDD, child)
      if functionsWithoutDistinct.forall(expr => expr.isInstanceOf[AggregateExpression])
        && functionsWithDistinct.size == 0 && !groupingExpressions.isEmpty && !hasJoin =>
      var physicalPlans = new ListBuffer[SparkPlan]
      var filterNode = child
      while (!filterNode.isInstanceOf[Filter] && !filterNode.isInstanceOf[LeafNode])
        filterNode = filterNode.children(0)

      //todo multiple sketches

      //make rect as close ranges

      val hyperRects = if (filterNode.isInstanceOf[Filter]) Seq(Seq(filterNode.asInstanceOf[Filter].condition.asInstanceOf[And]))
      else null
      //todo asnwer with CMS
      /*if( hyperRects.forall(x=>x.forall(y=>y.left.asInstanceOf[BinaryComparison].right.asInstanceOf[Literal].value
        ==y.right.asInstanceOf[BinaryComparison].right.asInstanceOf[Literal].value)))
      print()*/
      physicalPlans += GroupAggregateSketchExec(confidence, error, seed, groupingExpressions, resultExpressions, functionsWithoutDistinct, hyperRects, logicalRDD)
    //todo mdr is slow
    case ApproximatePhysicalAggregation(confidence, error, seed, hasJoin, groupingExpressions
    , functionsWithDistinct: Seq[AggregateExpression], functionsWithoutDistinct: Seq[AggregateExpression]
    , resultExpressions, logicalRDD, child)
      if functionsWithoutDistinct.forall(expr => expr.isInstanceOf[AggregateExpression])
        && functionsWithDistinct.size == 0 && groupingExpressions.isEmpty && !hasJoin =>
      var physicalPlans = new ListBuffer[SparkPlan]
      var filterNode = child
      while (!filterNode.isInstanceOf[Filter] && !filterNode.isInstanceOf[LeafNode])
        filterNode = filterNode.children(0)
      if (!filterNode.isInstanceOf[Filter]) {
        NonGroupAggregateSketchExec(confidence, error, seed, resultExpressions, functionsWithoutDistinct, null, logicalRDD)
      }
      //make rect as close ranges
      val hyperRects = combineHyperRectangles(filterNode.asInstanceOf[Filter].condition)
      //if(hyperRects.forall(x=>x.forall(y=>y.right.asInstanceOf[Literal].value==y.left.asInstanceOf[Literal].value)))

      //physicalPlans++=getPossibleCMSPlan()
      //val sketches = new ListBuffer[MultiDyadicRangeExec]
      physicalPlans += NonGroupAggregateSketchExec(confidence, error, seed, resultExpressions, functionsWithoutDistinct, hyperRects, logicalRDD)
      physicalPlans

    case PhysicalAggregation(groupingExpressions, aggExpressions, resultExpressions, child)
      if aggExpressions.forall(expr => expr.isInstanceOf[AggregateExpression]) =>
      val aggregateExpressions = aggExpressions.map(expr =>
        expr.asInstanceOf[AggregateExpression])

      val (functionsWithDistinct, functionsWithoutDistinct) =
        aggregateExpressions.partition(_.isDistinct)
      if (functionsWithDistinct.map(_.aggregateFunction.children.toSet).distinct.length > 1) {
        // This is a sanity check. We should not reach here when we have multiple distinct
        // column sets. Our `RewriteDistinctAggregates` should take care this case.
        sys.error("You hit a query analyzer bug. Please report your query to " +
          "Spark user mailing list.")
      }
      var physicalPlans = new ListBuffer[SparkPlan]
      if (groupingExpressions.isEmpty) {
        for (aggExpression <- aggregateExpressions) {
          if (!aggExpression.isDistinct && aggExpression.aggregateFunction.isInstanceOf[Count]) {
            //todo children size ???
            //todo two filter ???
            var sketchLogicalRDD = plan.children(0)
            var sketchCondition: Expression = null
            val sketchResults = resultExpressions
            var sketchProjectAtt: NamedExpression = null
            // todo what if no project
            while (!sketchLogicalRDD.isInstanceOf[LeafNode] && sketchLogicalRDD != null) {
              //todo count(*)
              if (sketchLogicalRDD.isInstanceOf[Project]) {
                sketchProjectAtt = sketchLogicalRDD.asInstanceOf[Project].projectList.filter(x
                => x.exprId == aggExpression.aggregateFunction.children(0).asInstanceOf[AttributeReference].exprId)(0)
              }
              if (sketchLogicalRDD.isInstanceOf[Filter])
                sketchCondition = sketchLogicalRDD.asInstanceOf[Filter].condition
              sketchLogicalRDD = sketchLogicalRDD.children(0)
            }
            val possibleSketches = getPossibleSketch(.9, 0.0001, 9223372036854775783L, sketchResults, sketchCondition
              , sketchProjectAtt, sketchLogicalRDD.asInstanceOf[LogicalRDD])
            val countMinSketchExec = null //CountMinSketchExec(1E-10, 0.001, 9223372036854775783L, sketchResults, sketchCondition, sketchProjectAtt, sketchLogicalRDD.asInstanceOf[LogicalRDD])
            if (resultExpressions.size > 1 && sketchResults.size > 0) {
              val aggregateOperator =
                if (functionsWithDistinct.isEmpty) {
                  AggUtils.planAggregateWithoutDistinct(
                    groupingExpressions,
                    aggregateExpressions.filter(x => (!x.aggregateFunction.isInstanceOf[Count])),
                    resultExpressions.filter(_.asInstanceOf[Alias].name.contains("count")),
                    planLater(child))
                } else {
                  AggUtils.planAggregateWithOneDistinct(
                    groupingExpressions,
                    functionsWithDistinct,
                    functionsWithoutDistinct,
                    //todo ??? what happen when it is distinct count
                    resultExpressions.filter(_.asInstanceOf[Alias].name.contains("count")),
                    planLater(child))
                }
              physicalPlans += ExtAggregateExec(groupingExpressions, aggregateExpressions, aggregateOperator ++ possibleSketches)
            }
            else
              for (plan <- possibleSketches)
                physicalPlans += (plan)
          }

        }
      }


      val aggregateOperator =
        if (functionsWithDistinct.isEmpty) {
          AggUtils.planAggregateWithoutDistinct(
            groupingExpressions,
            aggregateExpressions,
            resultExpressions,
            planLater(child))
        } else {
          AggUtils.planAggregateWithOneDistinct(
            groupingExpressions,
            functionsWithDistinct,
            functionsWithoutDistinct,
            resultExpressions,
            planLater(child))
        }
      // aggregateOperator //++
      physicalPlans
    case PhysicalAggregation(groupingExpressions, aggExpressions, resultExpressions, child)
      if aggExpressions.forall(expr => expr.isInstanceOf[PythonUDF]) =>
      val udfExpressions = aggExpressions.map(expr => expr.asInstanceOf[PythonUDF])

      Seq(execution.python.AggregateInPandasExec(
        groupingExpressions,
        udfExpressions,
        resultExpressions,
        planLater(child)))

    /*todo
    case PhysicalAggregation(_, _, _, _) =>
      // If cannot match the two cases above, then it's an error
      throw new AnalysisException(
        "Cannot use a mixture of aggregate function and group aggregate pandas UDF")
*/

    case _ => Nil
  }

  def getPossibleSketch(DELTA: Double
                        , EPS: Double
                        , SEED: Long
                        , resultExpressions: Seq[NamedExpression]
                        , conditions: Expression
                        , sketchProjectAtt: NamedExpression
                        //todo we should define scan based on other condition or resultExpression
                        , sketchLogicalRDD: LogicalRDD): Seq[SparkPlan] = {
    val sketches: ListBuffer[SparkPlan] = new ListBuffer[SparkPlan]
    val param = getValidParam(conditions)
    val CMSConditions = getCMSConditions(conditions)

    if (param != "" && CMSConditions != null)
      sketches += (CountMinSketchExec(DELTA, EPS, SEED, resultExpressions, CMSConditions, sketchProjectAtt, sketchLogicalRDD.asInstanceOf[LogicalRDD]))

    if (param != "") {
      val (notNulls, ranges) = getDyadicConditions(conditions)
      //  sketches += (DyadicRangeExec(DELTA, EPS, SEED, resultExpressions, ranges, notNulls, sketchProjectAtt, sketchLogicalRDD.asInstanceOf[LogicalRDD]))

    }
    sketches
  }

  def treeIterator(expression: Expression) = {
    val t = expression
    //  while
  }

  def getCMSConditions(expression: Expression): Seq[Expression] = {
    //todo what if no expression?
    val t = expression
    while (t != null) {
      if (t.isInstanceOf[BinaryComparison]) {
        if ((t.isInstanceOf[EqualTo] || t.isInstanceOf[EqualNullSafe]) && (t.children(0).isInstanceOf[Literal] || t.children(1).isInstanceOf[Literal]))
          return Seq(t.asInstanceOf[BinaryComparison])
      }
      else if (t.isInstanceOf[IsNull] || t.isInstanceOf[IsNotNull]) {
        return Seq(t)
      }
      else if (t.isInstanceOf[And]) {
        val left = getCMSConditions(t.asInstanceOf[BinaryOperator].left)
        val right = getCMSConditions(t.asInstanceOf[BinaryOperator].right)
        if (left == null || right == null)
          return null
        if (right(0).isInstanceOf[IsNotNull] || left(0).isInstanceOf[IsNotNull])
          return left.++:(right)
        if (right(0).isInstanceOf[IsNull])
          return right
        if (left(0).isInstanceOf[IsNull])
          return left
        //todo we have e=1 and e=2 ridiculous
        return null
      }
      else if (t.isInstanceOf[Or]) {
        val left = getCMSConditions(t.asInstanceOf[BinaryOperator].left)
        val right = getCMSConditions(t.asInstanceOf[BinaryOperator].right)
        if (left == null || right == null)
          return null
        return left.++:(right)
      }
      return null
    }
    return null
  }

  def getValidParam(expression: Expression): String = {
    val t = expression
    while (t != null) {
      if (t.isInstanceOf[AttributeReference])
        if (t.dataType.isInstanceOf[NumericType] || t.dataType.isInstanceOf[StringType])
          return t.asInstanceOf[AttributeReference].name
        else return ""
      else if (t.isInstanceOf[UnaryExpression]) {
        return getValidParam(t.children(0))
      }
      else if (t.isInstanceOf[Literal]) {
        return null
      }
      else {
        val left = getValidParam(t.asInstanceOf[BinaryOperator].left)
        val right = getValidParam(t.asInstanceOf[BinaryOperator].right)
        if (left == "" || right == "")
          return ""
        if (left == null)
          return right
        if (right == null)
          return left
        if ((left == right && left != ""))
          return left
        return ""
      }
    }
    ""
  }

  def getDyadicConditions(conditions: Expression): (Seq[UnaryExpression], Seq[And]) = {

    (getIsNotNull(conditions), combineRanges(conditions))
  }

  def getIsNotNull(expression: Expression): Seq[UnaryExpression] = {
    if (expression != null)
      if (expression.isInstanceOf[IsNotNull])
        return Seq(expression.asInstanceOf[UnaryExpression])
      else if (expression.isInstanceOf[And] || expression.isInstanceOf[Or]) {
        return getIsNotNull(expression.asInstanceOf[BinaryOperator].right).++:(getIsNotNull(expression.asInstanceOf[BinaryOperator].left))

      }
    Seq()
  }

  //todo make close range
  def convertBCToAnd(inExpression: BinaryComparison): And = {
    var expression: BinaryComparison = null
    if (inExpression.left.isInstanceOf[Literal]) {
      if (inExpression.isInstanceOf[LessThan])
        expression = GreaterThan(inExpression.right, inExpression.left)
      else if (inExpression.isInstanceOf[LessThanOrEqual])
        expression = GreaterThanOrEqual(inExpression.right, inExpression.left)
      else if (inExpression.isInstanceOf[GreaterThan])
        expression = LessThan(inExpression.right, inExpression.left)
      else if (inExpression.isInstanceOf[GreaterThanOrEqual])
        expression = LessThanOrEqual(inExpression.right, inExpression.left)
    }
    else
      expression = inExpression
    //todo set max and min
    if (expression.isInstanceOf[LessThan] || expression.isInstanceOf[LessThanOrEqual])
      return And(GreaterThanOrEqual(getAttributeReference(expression), new Literal(0, expression.left.dataType)), expression)
    if (expression.isInstanceOf[GreaterThan] || expression.isInstanceOf[GreaterThanOrEqual])
      return And(expression, LessThanOrEqual(getAttributeReference(expression), new Literal(10000000, expression.left.dataType)))
    if (expression.isInstanceOf[EqualTo] || expression.isInstanceOf[EqualNullSafe]) {
      val (l, e) = if (expression.left.isInstanceOf[Literal]) (expression.left, expression.right) else (expression.right, expression.left)
      return And(GreaterThanOrEqual(e, l), LessThanOrEqual(e, l))
    }
    And(null, null)
  }


  def getLiteral(expression: BinaryComparison): Literal = {
    if (expression.left.isInstanceOf[Literal])
      expression.left.asInstanceOf[Literal]
    if (expression.right.isInstanceOf[Literal])
      expression.right.asInstanceOf[Literal]
    null
  }

  def getAttributeReference(expression: BinaryComparison): AttributeReference = {
    if (expression.left.isInstanceOf[AttributeReference])
      return expression.left.asInstanceOf[AttributeReference]
    if (expression.right.isInstanceOf[AttributeReference])
      return expression.right.asInstanceOf[AttributeReference]
    null
  }


  def getRangeIntValue(in: BinaryComparison): Int = {
    if (in.left.isInstanceOf[Literal])
      if (in.isInstanceOf[GreaterThan])
        in.left.asInstanceOf[Literal].value.toString.toDouble.toInt - 1
      else if (in.isInstanceOf[LessThan])
        in.left.asInstanceOf[Literal].value.toString.toDouble.toInt + 1
      else
        in.left.asInstanceOf[Literal].value.toString.toDouble.toInt
    else {
      if (in.isInstanceOf[GreaterThan])
        in.right.asInstanceOf[Literal].value.toString.toDouble.toInt + 1
      else if (in.isInstanceOf[LessThan])
        in.right.asInstanceOf[Literal].value.toString.toDouble.toInt - 1
      else
        in.right.asInstanceOf[Literal].value.toString.toDouble.toInt
    }
  }

  def unionRangesWithRanges(rs1: Seq[And], rs2: Seq[And]): Seq[And] = {
    var out: Seq[And] = rs1
    for (range <- rs2) {
      out = (unionRangeWithRanges(range, out))
    }
    out
  }

  def unionRangeWithRanges(rr: And, rss: Seq[And]): Seq[And] = {
    var flag = true
    var r = rr
    var rs = rss
    var out: Seq[And] = Seq()
    while (flag) {
      var can: And = null
      flag = false
      for (i <- (0 to rs.size - 1)) {
        val r1_l = getRangeIntValue(r.left.asInstanceOf[BinaryComparison])
        val r1_r = getRangeIntValue(r.right.asInstanceOf[BinaryComparison])
        val r2_l = getRangeIntValue(rs(i).left.asInstanceOf[BinaryComparison])
        val r2_r = getRangeIntValue(rs(i).right.asInstanceOf[BinaryComparison])
        if (!(r1_r < r2_l || r2_r < r1_l)) {
          rs = rs.++:(unionRangeWithRange(r, rs(i)))
          rs = rs.drop(i)
          flag = true
        }
      }
      if (!flag)
        out = rs.++:(Seq(r))
    }
    out
  }

  def unionRangeWithRange(r1: And, r2: And): Seq[And] = {
    val r1_l = getRangeIntValue(r1.left.asInstanceOf[BinaryComparison])
    val r1_r = getRangeIntValue(r1.right.asInstanceOf[BinaryComparison])
    val r2_l = getRangeIntValue(r2.left.asInstanceOf[BinaryComparison])
    val r2_r = getRangeIntValue(r2.right.asInstanceOf[BinaryComparison])
    if (r1_r < r2_l || r2_r < r1_l)
      Seq(r1, r2)
    if (r1_l > r2_l) {
      if (r1_r < r2_r)
        Seq(r2)
      else
        Seq(And(GreaterThanOrEqual(getAttributeReference(r2.left.asInstanceOf[BinaryComparison]), Literal(r2_l, IntegerType))
          , LessThanOrEqual(getAttributeReference(r1.right.asInstanceOf[BinaryComparison]), Literal(r1_r, IntegerType))))
    } else {
      if (r1_r < r2_r)
        Seq(And(GreaterThanOrEqual(getAttributeReference(r1.left.asInstanceOf[BinaryComparison]), Literal(r1_l, IntegerType))
          , LessThanOrEqual(getAttributeReference(r2.right.asInstanceOf[BinaryComparison]), Literal(r2_r, IntegerType))))
      else
        Seq(r1)
    }
  }

  def mergeOrsBinaryComparison(e1: BinaryComparison, e2: BinaryComparison): Seq[And] = {
    val a1 = convertBCToAnd(e1)
    val a2 = convertBCToAnd(e2)
    val l1 = getLiteral(e1).value.toString.toDouble
    val l2 = getLiteral(e2).value.toString.toDouble

    if (l1 > l2) {
      if (a1.right == null) {
        if (a2.right == null)
          Seq(a2)
        else
          null
      }
      else if (a1.left == null) {
        if (a2.left == null)
          Seq(a1)
        else
          Seq(And(null, null))
      }
      else {
        if (a2.right == null)
          Seq(a2)
        else
          Seq(a1, a2)
      }
    }
    else {
      if (a2.right == null) {
        if (a1.right == null)
          Seq(a1)
        else
          null
      }
      else if (a2.left == null) {
        if (a1.left == null)
          Seq(a2)
        else
          Seq(And(null, null))
      }
      else {
        if (a1.right == null)
          Seq(a1)
        else
          Seq(a2, a1)
      }
    }
  }
}

object ApproximatePhysicalAggregation {
  type ReturnType =
    (Double, Double, Long, Boolean, Seq[NamedExpression], Seq[Expression], Seq[Expression], Seq[NamedExpression], LogicalRDD, LogicalPlan)

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
      var plan = child
      var logicalRDD: LogicalRDD = if (child.isInstanceOf[LogicalRDD]) child.asInstanceOf[LogicalRDD] else null
      var hasJoin = false
      while (!plan.isInstanceOf[Join] && !plan.isInstanceOf[LeafNode]) {
        if (plan.isInstanceOf[LogicalRDD])
          logicalRDD = plan.asInstanceOf[LogicalRDD]
        plan = plan.children(0)
      }
      if (plan.isInstanceOf[LogicalRDD])
        logicalRDD = plan.asInstanceOf[LogicalRDD]
      if (plan.isInstanceOf[Join])
        hasJoin = true
      Some((confidence, error, seed, hasJoin,
        namedGroupingExpressions.map(_._2),
        functionsWithDistinct,
        functionsWithoutDistinct,
        rewrittenResultExpressions,
        logicalRDD,
        child))

    case _ => None
  }

}

