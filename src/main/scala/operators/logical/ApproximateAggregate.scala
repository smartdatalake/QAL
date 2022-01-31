package operators.logical

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression, WindowExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}

case class ApproximateAggregate(confidence: Double, error: Double, seed: Long,
                                groupingExpressions: Seq[Expression],
                                aggregateExpressions: Seq[Expression],
                                output: Seq[Attribute],
                                child: LogicalPlan)
  extends UnaryNode {

  //override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute)

  override def maxRows: Option[Long] = child.maxRows

  override def validConstraints: Set[Expression] = {
    val nonAgg = aggregateExpressions.filter(_.find(_.isInstanceOf[AggregateExpression]).isEmpty)
    child.constraints.union(getAliasedConstraints(nonAgg.map(_.asInstanceOf[NamedExpression])))
  }

  override def toString() = {
    "Approximated(confidence " + confidence + " error " + error + " " + super.toString()
  }

}


case class ApproximateDistinctJoin(confidence: Double, error: Double, seed: Long, aggregateExpressions: Seq[AggregateExpression]
                                   , groupingExpression: Seq[NamedExpression], child: LogicalPlan)
  extends UnaryNode {

  override def output: Seq[Attribute] = child.output.map(_.toAttribute)

  override def maxRows: Option[Long] = child.maxRows

  override def validConstraints: Set[Expression] = {
    val nonAgg = child.output.filter(_.find(_.isInstanceOf[AggregateExpression]).isEmpty)
    child.constraints.union(getAliasedConstraints(nonAgg))
  }

  override def toString() = {
    "ApproximatedDistinctJoin(confidence " + confidence + " error " + error + " " + super.toString()
  }
}