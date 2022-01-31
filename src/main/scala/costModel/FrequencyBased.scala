/*package costModel

import definition.Paths.{ getAggSubQueries, updateAttributeName, windowSize}
import org.apache.spark.sql.catalyst.plans.logical.ReturnAnswer
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

import scala.collection.{Seq, mutable}
import org.apache.spark.sql.SparkSession

class FrequencyBased(maxSpace:Long , sparkSession: SparkSession) extends CostModelAbs() {
  val future = new mutable.Queue[Seq[Seq[Seq[UnaryExecNode]]]]() // ( q1( sub1(appSynopses1,appSynopses2) , sub2(appSynopses1,appSynopses2) ),  q2( sub1(appSynopses1,appSynopses2) , sub2(appSynopses1,appSynopses2) ) )
  var currentSubQueriesAPP = Seq[Seq[SparkPlan]]() // ( sub1(app1,app2) , sub2(app1,app2) )

  override def suggest(): Seq[SparkPlan] = {
    val currentQuerySubQueryAPP_Synopses = currentSubQueriesAPP.map(subQuery => subQuery
      .map(subQueryAPP => (subQueryAPP, extractSynopses(subQueryAPP)))) // ( ((supQuery1APP1,Synopses),(supQuery1APP2,Synopses)) ,  ((supQuery2APP1,Synopses),(supQuery2APP1,Synopses)))
    val currentQuerySubQueryAPP_SynopsesFRQ = currentQuerySubQueryAPP_Synopses.map(subQueryAPP_Ses => {
      subQueryAPP_Ses.map(subQueryAPP_S => {
        val synopsesCoverage = future.map(fQuery => fQuery.map(subQuery => subQuery.map(subQuerySynopses => coverage(subQueryAPP_S._2, subQuerySynopses)).max).reduce(_ + _)).reduce(_ + _)
        (subQueryAPP_S._1, subQueryAPP_S._2, synopsesCoverage)
      }).reduce(reducer)
    })
    currentQuerySubQueryAPP_SynopsesFRQ.map(_._1)
  }

  def reducer(a: (SparkPlan, Seq[UnaryExecNode], Int), b: (SparkPlan, Seq[UnaryExecNode], Int)): (SparkPlan, Seq[UnaryExecNode], Int) = {
    if (a._3 > b._3)
      a
    else if (a._3 < b._3)
      b
    else if (a._2.map(x => costOfAppPlan(x, Seq())._2).reduce(_ + _) < b._2.map(x => costOfAppPlan(x, Seq())._2).reduce(_ + _))
      a
    else
      b
  }

  override def addQuery(query: String): Unit = {
    if (future.size > windowSize)
      future.dequeue()
    future.enqueue(currentSubQueriesAPP.map(subQuery => subQuery.map(extractSynopses)))
    currentSubQueriesAPP = getAggSubQueries(sparkSession.sqlContext.sql(query).queryExecution.analyzed).map(subQuery => {
      updateAttributeName(subQuery, new mutable.HashMap[String, Int]())
      val joins = enumerateRawPlanWithJoin(subQuery)
      val logicalPlans = joins.map(x => sparkSession.sessionState.optimizer.execute(x))
      logicalPlans.flatMap(x => sparkSession.sessionState.planner.plan(ReturnAnswer(x)))
    })
  }

  override def getFutureAPP(): Seq[Seq[Seq[SparkPlan]]] = null

  override def getFutureSize(): Long = ???

}*/
