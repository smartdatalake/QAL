package operators.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}

case class CountMinSketch(
                     DELTA: Double,
                     EPS: Double,
                     SEED: Long,
                     child: LogicalPlan,
                     outputExpression:NamedExpression,
                     condition: Expression) extends UnaryNode with Serializable {
  override def output: Seq[Attribute] = Seq(outputExpression.toAttribute)

}

