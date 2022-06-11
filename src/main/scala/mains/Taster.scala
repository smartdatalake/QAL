package mains

import rules.logical.{ApproximateInjector, pushFilterUp}
import rules.physical.{SampleTransformation, SampleTransformationMultiple}
import costModel.TasterCostModel
import definition.Paths._

import scala.collection.Seq

object Taster extends QueryEngine_Abs("Taster") {

  def main(args: Array[String]): Unit = {

    readConfiguration(args)
    val costModel = new TasterCostModel(sparkSession, justAPP = justAPP, isAdaptive = isAdaptive)
    loadTables(sparkSession)
    queries = loadWorkloadWithIP("skyServer", sparkSession)
    mapRDDScanRowCNT = readRDDScanRowCNT(sparkSession)

    sparkSession.experimental.extraStrategies = Seq(SampleTransformation)
    sparkSession.experimental.extraOptimizations = Seq(new ApproximateInjector(confidence, error, seed), new pushFilterUp)


    execute(costModel)

    flush()
  }



  override def ReadNextQueries(query: String, ip: String, epoch: Long, i: Int): Seq[String]
  = queries.slice((if (i - (windowSize ) < 0) 0 else i - (windowSize )), i).map(_._1).reverse


  /*
  *         updateAttributeName(subQuery, new mutable.HashMap[String, Int]())
        val joins = enumerateRawPlanWithJoin(subQuery)
        val logicalPlans = joins.map(x => sparkSession.sessionState.optimizer.execute(x))
        val physicalPlans = logicalPlans.flatMap(x => sparkSession.sessionState.planner.plan(ReturnAnswer(x)))
        val gainOfCandidateAPP = physicalPlans.map(pp => {
          val SynopsesAndEstimatedSize = extractSynopses(pp).map(y => (y.toString(), estimateSizeOf(y)))
          println((if (futureAppPhysicalPlans.size > 0) futureAppPhysicalPlans.map(PPsOfEachQuery => PPsOfEachQuery.map(pp
          => costOfPlan(pp, SynopsesAndEstimatedSize)._2).min).reduce(_ + _) else 0))
          (pp, (costOfPlan(pp, Seq())._2
            + (if (futureAppPhysicalPlans.size > 0) futureAppPhysicalPlans.map(PPsOfEachQuery => PPsOfEachQuery.map(pp
          => costOfPlan(pp, SynopsesAndEstimatedSize)._2).min).reduce(_ + _) else 0)), extractSynopses(pp))
        })*/
  override def readConfiguration(args: Array[String]): Unit = super.readConfiguration(args)
}
