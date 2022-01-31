package mains

import costModel.TasterCostModel
import definition.Paths._
import rules.logical.{ApproximateInjector, pushFilterUp}
import rules.physical.SampleTransformationMultiple

import scala.collection.Seq

object Taster extends QueryEngine_Abs("Taster") {

  def main(args: Array[String]): Unit = {

    readConfiguration(args)
    val costModel = new TasterCostModel(sparkSession, justAPP = justAPP, isAdaptive = isAdaptive)
    loadTables(sparkSession)
    val queries = loadWorkloadWithIP("skyServer", sparkSession)
    mapRDDScanRowCNT = readRDDScanRowCNT(sparkSession)

    // if(!isExtended)
    //  sparkSession.experimental.extraStrategies = Seq(SampleTransformationMultiple)
    //  else
    //  if (isExtended)
    sparkSession.experimental.extraStrategies = Seq(SampleTransformationMultiple)
    // else sparkSession.experimental.extraStrategies = Seq(SampleTransformation)

    sparkSession.experimental.extraOptimizations = Seq(new ApproximateInjector(confidence, error, seed), new pushFilterUp)


    execute(queries, costModel)

    printReport(results)
    flush()
  }


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
