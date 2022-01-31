package mains

import costModel.PredictiveCostModel
import definition.Paths._
import rules.logical.{ApproximateInjector, pushFilterUp}
import rules.physical.SampleTransformationMultiple


object Adaptive extends QueryEngine_Abs("MLLSTM") {


  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  def main(args: Array[String]): Unit = {
    readConfiguration(args)
    loadTables(sparkSession)
    mapRDDScanRowCNT = readRDDScanRowCNT(sparkSession)
    //sparkSession.sql("select ra, count(z) as num_redshift from specobj where z between 0.5 and 1 group by ra").show(100000000)
    val queries = loadWorkloadWithIP("skyServer", sparkSession)

    val costModel = new PredictiveCostModel(sparkSession, isLRU, justAPP)
    //  if (isExtended)
    sparkSession.experimental.extraStrategies = Seq(SampleTransformationMultiple)
    // else sparkSession.experimental.extraStrategies = Seq(SampleTransformation)
    sparkSession.experimental.extraOptimizations = Seq(new ApproximateInjector(confidence, error, seed), new pushFilterUp)
    queryCNT = 0
    var i=1
    for (query <- queries) if (queryCNT <= testSize) {
      var checkpointForAppQueryExecution = System.nanoTime()
      var checkpointForAppQueryPlanning = System.nanoTime()
      var checkPointForSampling: Long = 0
      var checkAct: Long = 0
      var checkPointForExecution: Long = 0
        println(i+"qqqqqqqqqqqqqqqqqqqqqqqqqq" + query)
i+=1
      costModel.addQuery(query._1, query._2.replace("\t", ""), query._3)
      //  println("----------------------------------")
      outputOfQuery = ""
      val appPhysicalPlan = costModel.suggest()
      // appPhysicalPlan.foreach(x => println(toStringTree(x)))
      checkpointForAppQueryPlanning = (System.nanoTime() - checkpointForAppQueryPlanning) / 1
      for (subqueryAPP <- appPhysicalPlan) {
        //println( costModel.getFutureProjectList())
        var cheapest = changeSynopsesWithScan(subqueryAPP)
        if (isExtended)
          cheapest = ExtendProject(cheapest, costModel.getFutureProjectList().distinct)

        var t = System.nanoTime()
        val tt = System.nanoTime()
        // println(cheapest)
        executeAndStoreSample(cheapest, sparkSession)
        cheapest = changeSynopsesWithScan(cheapest)
        //println(cheapest)
        checkPointForSampling += (System.nanoTime() - t) / 1
        countReusedSample(cheapest)
        t = System.nanoTime()
        cheapest = prepareForExecution(cheapest, sparkSession)
        //   println(cheapest)
        cheapest.executeCollectPublic().toList.foreach(row => {
          outputOfQuery += row.toString()
          counterNumberOfGeneratedRow += 1
        })
        checkPointForExecution += (System.nanoTime() - tt) / 1
        checkAct += (System.nanoTime() - t) / 1
      }
      queryCNT += 1
      var checkWare = System.nanoTime()
      costModel.updateWarehouse()
      checkWare = (System.nanoTime() - checkWare) / 1
      // tableName.clear()
      checkpointForAppQueryExecution = (System.nanoTime() - checkpointForAppQueryExecution) / 1
      //   println(checkpointForAppQueryExecution)
      results += ((query._1, outputOfQuery, checkpointForAppQueryExecution, checkpointForAppQueryPlanning
        , checkPointForSampling, checkAct, checkWare, checkPointForExecution))
      val agg = results.reduce((a, b) => ("", "", a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7, a._8 + b._8))
      println(checkPointForExecution/1000000000+","+agg._8/1000000000)
    }
    printReport(results)
    flush()
  }


  override def readConfiguration(args: Array[String]): Unit = {
    super.readConfiguration(args)

  }
}
