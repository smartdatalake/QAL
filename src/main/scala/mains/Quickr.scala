package mains

import costModel.LRUCostModel
import definition.Paths._
import rules.logical.{ApproximateInjector, pushFilterUp}
import rules.physical.SampleTransformation


object Quickr extends QueryEngine_Abs("Quickr") {


  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  def main(args: Array[String]): Unit = {
    readConfiguration(args)

    loadTables(sparkSession)
    mapRDDScanRowCNT = readRDDScanRowCNT(sparkSession)
    val queries = loadWorkloadWithIP("skyServer", sparkSession)
    //sparkSession.sqlContext.sql("select count(*) from photoprimary as p left outer join photoz as z on p.objid = z.objid where (p.dered_r < 21.0) and ((p.type_g = 3) or (p.type_r = 3) or (p.type_i = 3)) and z.z > 0.224 and z.z < 0.324 and p.ra between 135.358951321 and 135.567498679 and p.dec between 34.8524513215 and 35.0609986785").collect().toList.toString()
    sparkSession.experimental.extraStrategies = Seq(SampleTransformation);
    sparkSession.experimental.extraOptimizations = Seq(new ApproximateInjector(confidence, error, seed), new pushFilterUp());
    val costModel = new LRUCostModel(sparkSession)
    for (query <- queries) if (queryCNT <= testSize) {
      var checkpointForAppQueryExecution = System.nanoTime()
      var checkpointForAppQueryPlanning = System.nanoTime()
      var checkPointForExecution: Long = 0
      //costModel.addQuery(query._1, query._3)
      outputOfQuery = ""
      // println("qqqqqqqqqq" + query)
      val appPhysicalPlan = costModel.suggest()
      // appPhysicalPlan.foreach(x => println(toStringTree(x)))
      checkpointForAppQueryPlanning = (System.nanoTime() - checkpointForAppQueryPlanning) / 1
      for (subqueryAPP <- appPhysicalPlan) {
        var cheapest = ExtendProject(subqueryAPP, costModel.getFutureProjectList())
        val tt = System.nanoTime()
        //  println(cheapest)
        cheapest = prepareForExecution(cheapest, sparkSession)
        //   println(cheapest)
        cheapest.executeCollectPublic().toList.foreach(row => {
          outputOfQuery += row.toString()
          counterNumberOfGeneratedRow += 1
        })
        checkPointForExecution += (System.nanoTime() - tt) / 1
      }
      queryCNT += 1
      checkpointForAppQueryExecution = (System.nanoTime() - checkpointForAppQueryExecution) / 1
      //  checkpointForAppQueryExecution
      results += ((query._1, outputOfQuery, checkpointForAppQueryExecution, checkpointForAppQueryPlanning
        , 0, 0, 0, checkPointForExecution))
    }

    printReport()
    flush()
  }

  override def ReadNextQueries(query: String, ip: String, epoch: Long, queryIndex: Int): Seq[String] = null

  override def readConfiguration(args: Array[String]): Unit = {super.readConfiguration(args)
  }
}
