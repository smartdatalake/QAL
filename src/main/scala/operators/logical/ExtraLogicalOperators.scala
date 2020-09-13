package operators.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LeafNode, LogicalPlan}

case class Combiner (
left: LogicalPlan
, right:LogicalPlan
, groupingExpressions: Seq[Expression]
, aggregateExpressions: Seq[NamedExpression]) extends  BinaryNode {

  override def output: Seq[Attribute] = (left.output ++ right.output)

}
case class End() extends LeafNode {
  override def output: Seq[Attribute] = Nil
}
