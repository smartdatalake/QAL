package costModel

import org.apache.spark.sql.SparkSession

class SDL(sparkSession: SparkSession) extends CostModelAbs() {
  def addQuery(query: String, ip: String, epoch: Long, f: Seq[String]): Unit = ???

  val future: Seq[Seq[Seq[org.apache.spark.sql.execution.SparkPlan]]] = ???

  def getFutureAPP(): Seq[Seq[Seq[org.apache.spark.sql.execution.SparkPlan]]] = ???

  def getFutureSize(): Long = ???

  val setSelectionStrategy: costModel.bestSetSelector.BestSetSelectorAbs = ???

  def suggest(): Seq[org.apache.spark.sql.execution.SparkPlan] = ???

  def updateWarehouse(): Unit = ???

}
