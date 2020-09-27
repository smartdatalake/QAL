package extraSQLOperators

import operators.logical.Quantile
import operators.physical.QuantileSampleExec
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.Seq

object extraRulesWithoutSampling extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case q@Quantile(quantileColAtt, quantilePart, confidence, error, seed, child) =>
      Seq(QuantileSampleExec(quantileColAtt, quantilePart, planLater(child)))
    case _ => Nil
  }
}
