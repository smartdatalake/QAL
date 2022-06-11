package mains

import costModel.StaticCostModel
import rules.logical.{ApproximateInjector, pushFilterUp}
import rules.physical.SampleTransformationMultiple
import definition.Paths._

object Utopia extends QueryEngine_Abs("Utopia") {

  def main(args: Array[String]): Unit = {

    readConfiguration(args)
    val costModel = new StaticCostModel(sparkSession, justAPP = justAPP, isExtended = isExtended, isAdaptive = isAdaptive)
    loadTables(sparkSession)
    //  sparkSession.sqlContext.sql("select specobjid,count(*) from galaxy group by specobjid having count(*) >10").show(100000)
    val queries = loadWorkloadWithIP("skyServer", sparkSession)
    mapRDDScanRowCNT = readRDDScanRowCNT(sparkSession)

    sparkSession.experimental.extraStrategies = Seq(SampleTransformationMultiple)
    sparkSession.experimental.extraOptimizations = Seq(new ApproximateInjector(confidence, error, seed), new pushFilterUp())


    execute(costModel)
    flush()
  }

  override def ReadNextQueries(query: String, ip: String, epoch: Long, i: Int): Seq[String] = queries.slice(i - 1, i - 1 + windowSize).map(_._1)

  override def readConfiguration(args: Array[String]): Unit = super.readConfiguration(args)
}