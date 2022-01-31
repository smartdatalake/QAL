package costModel.bestSetSelector

import costModel.CostModelAbs
import definition.Paths.{ParquetNameToSynopses, warehouseParquetNameToSize}

import scala.collection.Seq

class CELF(costModel: CostModelAbs) extends BestSetSelectorAbs(costModel, true) {
  override def decide(): (Seq[(String, Long)], Seq[String]) = {
    A.clear()
    sizeOfA = 0
    VMinusA = warehouseParquetNameToSize.map(x => (ParquetNameToSynopses(x._1), x._2)) // { (sample1,cost) }
    if (VMinusA.size == 0 || VMinusA.reduce((a, b) => (null, a._2 + b._2))._2 <= costModel.maxSpace
      || costModel.getFutureSize() == 0) {
      A.++=(VMinusA)
      return (VMinusA.toSeq, Seq())
    }
    val lazyForwardUC = new Greedy(costModel, true)
    val lazyForwardCB = new Greedy(costModel, false)
    val uc=lazyForwardUC.decide()
    val cb=lazyForwardCB.decide()
    if (lazyForwardUC.calAReward() > lazyForwardCB.calAReward()) (lazyForwardUC.A, uc._2)
    else (lazyForwardCB.A, cb._2)
  }
}
