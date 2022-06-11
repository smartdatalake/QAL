package costModel.bestSetSelector

import costModel.CostModelAbs
import definition.Paths.{ParquetNameToSynopses, warehouseParquetNameToSize}

import scala.collection.{Seq, mutable}

import scala.collection.mutable

class PriorityQueueTest {
  implicit val ord: Ordering[((String, Long), Long, Boolean)] = Ordering.by(_._2)

  var queue = mutable.PriorityQueue[((String, Long), Long, Boolean)]()

}

class LazyForwardGreedy(costModel: CostModelAbs, isUnitCost: Boolean) extends BestSetSelectorAbs(costModel, isUnitCost) {
  override def decide(): (Seq[(String, Long)], Seq[String]) = {
    A.clear()
    sizeOfA = 0
    VMinusA = warehouseParquetNameToSize.map(x => (ParquetNameToSynopses(x._1), x._2)) // { (sample1,cost) }
    if (VMinusA.size == 0 || VMinusA.reduce((a, b) => (null, a._2 + b._2))._2 <= costModel.maxSpace
      || costModel.getFutureSize() == 0) {
      A.++=(VMinusA)
      return (A, Seq())
    }
    val pQueue = (new PriorityQueueTest()).queue
    // var eachSynopsisMarginalRewardForCurrentA = VMinusA.map(x => (x, Long.MaxValue, false)).toSet // { ( (sample1,cost) , marginalReward , cur ) }
    pQueue.++=(VMinusA.map(x => (x, Long.MaxValue, false)).toSeq)
    var sStar: ((String, Long), Long, Boolean) = null
    PerQueryPerSubqueryPhysicalPlans = costModel.getFutureAPP()
    while (hasCandidate) {
      //eachSynopsisMarginalRewardForCurrentA = eachSynopsisMarginalRewardForCurrentA.map(x => (x._1, x._2, false))
      var doo = true
      while (doo) {
        sStar = pQueue.dequeue()
        //  sStar = eachSynopsisMarginalRewardForCurrentA.filter(x => x._1._2 + sizeOfA <= costModel.maxSpace)
        //    .maxBy(x => x._2 * (if (isUnitCost) 1 else 1 / x._1._2.toDouble))
        if (sStar._3) {
          A.+=(sStar._1)
          sizeOfA += sStar._1._2
          VMinusA.remove(sStar._1._1)
          //  eachSynopsisMarginalRewardForCurrentA = eachSynopsisMarginalRewardForCurrentA.-(sStar)
          doo = false
          val temp=pQueue.map(x => (x._1, x._2, false))
          pQueue.clear()
          pQueue.++=(temp)
        }
        else {
          // eachSynopsisMarginalRewardForCurrentA. += ((sStar._1, calMarginalReward(sStar._1).toLong, true))
          pQueue.enqueue((sStar._1, calMarginalReward(sStar._1).toLong, true))
        }
      }
    }
    (A, VMinusA.toSeq.map(_._1))
  }
}
