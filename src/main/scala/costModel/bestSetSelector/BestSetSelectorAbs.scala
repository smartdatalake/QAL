package costModel.bestSetSelector

import costModel.CostModelAbs
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.mutable.ListBuffer
import scala.collection.{Seq, mutable}

abstract class BestSetSelectorAbs(costModel: CostModelAbs, isUnitCost: Boolean) extends Serializable {

  val A: ListBuffer[(String, Long)] = ListBuffer[(String, Long)]()
  var VMinusA: mutable.HashMap[String, Long] = null
  var sizeOfA: Long = 0
  var PerQueryPerSubqueryPhysicalPlans: Seq[Seq[Seq[SparkPlan]]] = null

  def decide(): (Seq[(String, Long)], Seq[String]) // (kept, removed)

  def costOfAppPlan(pp: SparkPlan, synopsesCost: Seq[(String, Long)]): (Long, Long) = costModel.costOfAppPlan(pp, synopsesCost)

  def costOfAppWithFixedSynopses(pp: SparkPlan, synopsesCost: Seq[(String, Long)]): (Long, Long) = costModel.costOfAppWithFixedSynopses(pp, synopsesCost)

  def costOfExact(pp: SparkPlan): (Long, Long) = costModel.costOfExact(pp)

  def findLast[A](la: List[A])(f: A => Boolean): Option[A] =
    la.foldLeft(Option.empty[A]) { (acc, cur) =>
      if (f(cur)) Some(cur)
      else acc
    }


  protected def calMarginalReward(synopses: (String, Long)): Double = {
    val score = PerQueryPerSubqueryPhysicalPlans.map(subQueries => subQueries.map(pps => {
      pps.map(pp => costOfAppWithFixedSynopses(pp, A)).minBy(_._2)._2 - pps.map(pp => costOfAppWithFixedSynopses(pp, A ++ Seq(synopses))).minBy(_._2)._2
    }).reduce(_ + _)).reduce(_ + _)
    return (if (isUnitCost) 1.0 else 1.0 / synopses._2.toDouble) * score
    (if (isUnitCost) 1.0 else 1.0 / synopses._2.toDouble) * (PerQueryPerSubqueryPhysicalPlans.map(perSubqueryPhysicalPlan => perSubqueryPhysicalPlan.map(subQuery => subQuery.map(APP => costOfAppPlan(APP, A)._2).min).reduce(_ + _)).reduce(_ + _)
      - PerQueryPerSubqueryPhysicalPlans.map(perSubqueryPhysicalPlan => perSubqueryPhysicalPlan.map(subQuery => subQuery.map(APP => costOfAppPlan(APP, A ++ Seq(synopses))._2).min).reduce(_ + _)).reduce(_ + _)).toDouble
  }

  def hasCandidate(): Boolean = VMinusA.find(s => s._2 + sizeOfA <= costModel.maxSpace).isDefined

  def calAReward(): Double = if (A.size == 0 || PerQueryPerSubqueryPhysicalPlans == null || PerQueryPerSubqueryPhysicalPlans.size == 0) 0.0 else
    PerQueryPerSubqueryPhysicalPlans.map(subQueries => subQueries.map(pps => {
      pps.map(costOfExact).minBy(_._2)._2 - pps.map(pp => costOfAppWithFixedSynopses(pp, A)).minBy(_._2)._2
    }).reduce(_ + _)).reduce(_ + _)

  //costModel.getCheapestExactCostOf(PerQueryPerSubqueryPhysicalPlans) - PerQueryPerSubqueryPhysicalPlans.map(perSubqueryPhysicalPlan => perSubqueryPhysicalPlan.map(subQueryAPPs => subQueryAPPs.map(APP => costOfAppPlan(APP, A)._2).min).reduce(_ + _)).reduce(_ + _)


}
