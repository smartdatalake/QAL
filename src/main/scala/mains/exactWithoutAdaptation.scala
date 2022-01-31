package mains

import costModel.ExactCostModel


object exactWithoutAdaptation extends QueryEngine_Abs("NoApp") {

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def main(args: Array[String]): Unit = {
    readConfiguration(args)
    val costModel = new ExactCostModel(sparkSession)
    loadTables(sparkSession)
    val queries = loadWorkloadWithIP("skyServer", sparkSession)
   // sparkSession.sql("select  count(*) as count from photoobj p ").show(100000)
  //  throw new Exception
    execute(queries, costModel)
    printReport(results)
    flush()
  }

  override def readConfiguration(args: Array[String]): Unit = super.readConfiguration(args)
}

/*
      // other exact executor which is via sql()
      outputOfQuery = ""
      val checkpointForExactQueryExecution = System.nanoTime()
      sparkSession.sql(query).collect().toList.foreach(row => {
        outputOfQuery += row.toString()
        counterNumberOfRowGenerated += 1
      })
      results += ((outputOfQuery, (System.nanoTime() - checkpointForExactQueryExecution) / 1000000))
      queryCNT += 1
*/