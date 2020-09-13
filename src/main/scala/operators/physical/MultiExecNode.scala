package operators.physical

import org.apache.spark.sql.execution.SparkPlan

trait MultiExecNode extends SparkPlan {
  override def children: Seq[SparkPlan]
}
