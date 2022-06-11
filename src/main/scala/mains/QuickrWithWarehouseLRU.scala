package mains


import costModel.LRUCostModel
import org.apache.spark.sql.catalyst.plans.logical.ReturnAnswer
import rules.logical.ApproximateInjector
import rules.physical.SampleTransformation

import scala.collection.{Seq, mutable}
import definition.Paths._
import mains.Taster.{changeSynopsesWithScan, executeAndStoreSample, prepareForExecution, sparkSession}


object QuickrWithWarehouseLRU extends QueryEngine_Abs("QuickrWithWarehouseLRU") {

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  def main(args: Array[String]): Unit = {
    readConfiguration(args)

    loadTables(sparkSession)
    mapRDDScanRowCNT = readRDDScanRowCNT(sparkSession)
    val queries = loadWorkload("skyServer", sparkSession)
    val costModel = new LRUCostModel(sparkSession)
    sparkSession.experimental.extraStrategies = Seq(SampleTransformation);
    sparkSession.experimental.extraOptimizations = Seq(new ApproximateInjector(confidence, error, seed));
    for (query <- queries) if (queryCNT <= testSize) {
      outputOfQuery = ""
      //costModel.addQuery(query,"",0)
      val checkpointForAppQueryExecution = System.nanoTime()
      val prepareTime = System.nanoTime()
      val cheapestPhysicalPlan = costModel.suggest()
      for (subqueryAPP <- cheapestPhysicalPlan) {
        var plan = changeSynopsesWithScan(subqueryAPP)
        executeAndStoreSample(plan, sparkSession)
        plan = changeSynopsesWithScan(subqueryAPP)
        plan = prepareForExecution(plan, sparkSession)
        plan.executeCollectPublic().toList.foreach(row => {
          outputOfQuery += row.toString()
          counterNumberOfGeneratedRow += 1
        })
      }
      //  println(outputOfQuery)
      costModel.updateWarehouse()
      tableName.clear()
      queryCNT += 1
    }
    printReport()
    flush()
  }

  override def ReadNextQueries(query: String, ip: String, epoch: Long, queryIndex: Int): Seq[String] = null
  override def readConfiguration(args: Array[String]): Unit = ???
}
