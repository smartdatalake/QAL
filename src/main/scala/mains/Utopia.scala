package mains

import costModel.StaticCostModel
import definition.Paths._
import rules.logical.{ApproximateInjector, pushFilterUp}
import rules.physical.SampleTransformationMultiple

object Utopia extends QueryEngine_Abs("Utopia") {

  def main(args: Array[String]): Unit = {

    readConfiguration(args)
    val costModel = new StaticCostModel(sparkSession, justAPP = justAPP, isExtended = isExtended, isAdaptive = isAdaptive)
    loadTables(sparkSession)
    val queries = loadWorkloadWithIP("skyServer", sparkSession)
    mapRDDScanRowCNT = readRDDScanRowCNT(sparkSession)
    //  val view = sparkSession.read.parquet("/home/hamid/QAL/QP/skyServer/warehouse/samplecavwzqdtremjxxzcdrzm.obj")
    // sparkSession.sqlContext.createDataFrame(view.rdd, view.schema).createOrReplaceTempView("samplecavwzqdtremjxxzcdrzm");
    // println(sparkSession.sql("select class, sum(ra) from samplecavwzqdtremjxxzcdrzm group by class").queryExecution.sparkPlan)
    //  if (isExtended)
    sparkSession.experimental.extraStrategies = Seq(SampleTransformationMultiple)
    // else sparkSession.experimental.extraStrategies = Seq(SampleTransformation)
    sparkSession.experimental.extraOptimizations = Seq(new ApproximateInjector(confidence, error, seed), new pushFilterUp())


    var i = 1
    var future = queries.slice(i, i + (windowSize * (1 + alpha)).toInt).map(_._1).toSeq
    for (query <- queries) if (queryCNT <= testSize) {
      sparkSession.sqlContext.clearCache()
      future = queries.slice(i, i + (windowSize * (1 + alpha)).toInt).map(_._1).toSeq
      costModel.addQuery(query._1, query._2.replace("\t", ""), 0, future)
      outputOfQuery = ""
      println(i+"qqqqqqqqqqqqqqqqqq" + query)
      i += 1
      var checkpointForAppQueryExecution: Long = 0
      var checkpointForAppQueryPlanning: Long = 0
      var checkPointForSampling: Long = 0
      var checkPointForExecution: Long = 0
      var checkAct: Long = 0
      checkpointForAppQueryExecution = System.nanoTime()
      checkPointForSampling = 0
      checkPointForExecution = 0
      checkAct = 0

      checkpointForAppQueryPlanning = System.nanoTime()
      val appPhysicalPlan = costModel.suggest()
      checkpointForAppQueryPlanning = (System.nanoTime() - checkpointForAppQueryPlanning) / 1

      //   try {
      for (subqueryAPP <- appPhysicalPlan) {
        var cheapest = changeSynopsesWithScan(prepareForExecution(subqueryAPP, sparkSession))
        if (isExtended)
          cheapest = ExtendProject(cheapest, costModel.getFutureProjectList().distinct)
        // var cheapest = changeSynopsesWithScan( prepareForExecution(subqueryAPP, sparkSession))
        // print(cheapest)
        var t = System.nanoTime()
        val tt = System.nanoTime()
        executeAndStoreSample(cheapest, sparkSession)
        cheapest = changeSynopsesWithScan(cheapest)
        checkPointForSampling += (System.nanoTime() - t) / 1
        cheapest.transform({
          case s: operators.physical.SampleExec =>
            println("i did")
            s.child
        })
        countReusedSample(cheapest)
        t = System.nanoTime()
       //  println(cheapest)

        cheapest.executeCollectPublic().toList.sortBy(_.toString()).foreach(row => {
          //   println(row.toString())
          outputOfQuery += row.toString() + "\n"
          counterNumberOfGeneratedRow += 1
        })

        checkPointForExecution += (System.nanoTime() - tt) / 1
        checkAct += (System.nanoTime() - t) / 1
      }
      queryCNT += 1
      var checkWare = System.nanoTime()
      costModel.updateWarehouse()
      checkWare = (System.nanoTime() - checkWare) / 1
      checkpointForAppQueryExecution = (System.nanoTime() - checkpointForAppQueryExecution) / 1
      // checkpointForAppQueryExecution
      results += ((query._1, outputOfQuery, checkpointForAppQueryExecution, checkpointForAppQueryPlanning
        , checkPointForSampling, checkAct, checkWare, checkPointForExecution))
      val agg = results.reduce((a, b) => ("", "", a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7, a._8 + b._8))
      println(checkPointForExecution/1000000000+","+agg._8/1000000000)

    }
    printReport(results)
    flush()

  }


  override def readConfiguration(args: Array[String]): Unit = super.readConfiguration(args)
}
