package costModel.bestSetSelector

import costModel.CostModelAbs
import definition.Paths.{ParquetNameToSynopses, SynopsesToParquetName, lastUsedOfParquetSample, warehouseParquetNameToSize}

import scala.collection.Seq

class Greedy(costModel: CostModelAbs, isUnitCost: Boolean) extends BestSetSelectorAbs(costModel, isUnitCost) {

  override def decide(): (Seq[(String, Long)], Seq[String]) = {
    A.clear()
    sizeOfA = 0
    VMinusA = warehouseParquetNameToSize.map(x => (ParquetNameToSynopses(x._1), x._2)) // { (sample1,cost) }
    if (VMinusA.size == 0 || VMinusA.reduce((a, b) => (null, a._2 + b._2))._2 <= costModel.maxSpace
      || costModel.getFutureSize() == 0) {
      A.++=(VMinusA)
      return (A, Seq())
    }
    PerQueryPerSubqueryPhysicalPlans = costModel.getFutureAPP()
    while (hasCandidate) {
      var eachSynopsisMarginalRewardForCurrentA = VMinusA.map(synopsisInWarehouse // { ( (sample1,cost) ,marginalGain) }
      => (synopsisInWarehouse, calMarginalReward(synopsisInWarehouse))).toList.sortBy(_._2)(Ordering[Double].reverse)
      //lastUsedOfParquetSample.map(x => ((x._1, warehouseParquetNameToSize.get(x._1).get), x._2)).toList.sortBy(_._2).reverse
   //   SynopsesToParquetName
      if (eachSynopsisMarginalRewardForCurrentA.reduce((a, b) => (("", 0), a._2 + b._2))._2 <= 1.0)
        eachSynopsisMarginalRewardForCurrentA = eachSynopsisMarginalRewardForCurrentA.map(x => (x._1, lastUsedOfParquetSample.get(SynopsesToParquetName.get(x._1._1).get).get.toDouble)).sortBy(_._2).reverse
      val bestSynopsisOption = eachSynopsisMarginalRewardForCurrentA.find(x => x._1._2 + sizeOfA <= costModel.maxSpace)
      val best = bestSynopsisOption.get
      A.+=(best._1)
      sizeOfA += best._1._2
      VMinusA.remove(best._1._1)
    }
    (A, VMinusA.toSeq.map(_._1))
  }

}
