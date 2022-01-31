package costModel.bestSetSelector

import costModel.CostModelAbs
import definition.Paths.{ParquetNameToSynopses, lastUsedOfParquetSample, warehouseParquetNameToSize}


class LRU(costModel: CostModelAbs) extends BestSetSelectorAbs(costModel, false) {
  override def decide(): (Seq[(String, Long)], Seq[String]) = {
    A.clear()
    sizeOfA = 0
    VMinusA = warehouseParquetNameToSize.map(x => (ParquetNameToSynopses(x._1), x._2)) // { (sample1,cost) }
    if (VMinusA.size == 0 || VMinusA.reduce((a, b) => (null, a._2 + b._2))._2 <= costModel.maxSpace) {
      A.++=(VMinusA)
      return (A, Seq())
    }
    val p = lastUsedOfParquetSample.map(x => ((x._1, warehouseParquetNameToSize.get(x._1).get), x._2)).toList.sortBy(_._2).reverse
    var index = 0
    while (hasCandidate) {
      val best = p(index)
      if (best._1._2 + sizeOfA <= costModel.maxSpace) {
        A.+=(best._1)
        sizeOfA += best._1._2
        VMinusA.remove(ParquetNameToSynopses.get(best._1._1).get)
      }
      index += 1
    }
    (A, VMinusA.toSeq.map(_._1))
  }

}
