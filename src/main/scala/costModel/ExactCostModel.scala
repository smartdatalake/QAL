package costModel

import costModel.bestSetSelector.BestSetSelectorAbs
import definition.Paths.getAggSubQueries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.ReturnAnswer
import org.apache.spark.sql.execution.SparkPlan
import definition.Paths._

import scala.collection.mutable.ListBuffer
import scala.collection.{Seq, mutable}

class ExactCostModel(sparkSession: SparkSession) extends CostModelAbs {
  var currentSubQueriesAPP: Seq[Seq[SparkPlan]] = null // ( sub1(app1,app2) , sub2(app1,app2) )

  override val setSelectionStrategy: BestSetSelectorAbs = null

  override def updateWarehouse(): Unit = Unit

  override def getFutureSize(): Long = throw new Exception("err2: no future for exact cost model")

  override def getFutureAPP(): Seq[Seq[Seq[SparkPlan]]] = throw new Exception("err2: no app future plan for exact cost model")

  override def suggest(): Seq[SparkPlan] = currentSubQueriesAPP.map(x => x(0))

  override def UpdateWindowHorizon(): Int = 0

  override def addQuery(query: String, f: Seq[String] = null,futureProjectList:ListBuffer[String]): Unit = {
    updateAttributeName(sparkSession.sqlContext.sql(query).queryExecution.analyzed, new mutable.HashMap[String, Int]())
    currentSubQueriesAPP = getAggSubQueries(sparkSession.sqlContext.sql(query).queryExecution.analyzed)
      .map(pp => sparkSession.sessionState.planner.plan(ReturnAnswer(sparkSession.sessionState.optimizer.execute(pp))).toSeq)
  }
}
