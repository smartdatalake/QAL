package rules.logical

import operators.logical.ApproximateAggregate
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

class ApproximateInjector(conf:Double, err:Double, seed:Long) extends Rule[LogicalPlan] with PredicateHelper {
 def apply(plan: LogicalPlan): LogicalPlan = plan transform {
   case agg@Aggregate(groupExpr, aggrExpr,child) =>
      ApproximateAggregate(conf  ,err ,seed, groupExpr,aggrExpr,agg.output,child)
 }
}
