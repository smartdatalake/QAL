package rules.physical

import operators.logical.{ApproximateAggregate, ApproximateDistinctJoin, DistinctSample, Quantile, UniformSample, UniformSampleWithoutCI, UniversalSample}
import operators.physical.{DistinctSampleExec2, DyadicRangeExec, PProject, QuantileSampleExec, QuantileSketchExec, UniformSampleExec2, UniformSampleExec2WithoutCI, UniversalSampleExec2}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, EqualTo, EquivalentExpressions, Expression, NamedExpression, PythonUDF}
import org.apache.spark.sql.execution.aggregate.AggUtils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LeafNode, LogicalPlan, Project}

import scala.collection.Seq
import scala.collection.mutable.ListBuffer

object SampleTransformation2 extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case p@Project(pl, child) =>
      Seq(PProject(pl, planLater(child)))
    case _ => Nil
  }

}




