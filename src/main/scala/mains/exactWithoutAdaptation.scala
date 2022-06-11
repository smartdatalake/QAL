package mains

import costModel.ExactCostModel
import rules.logical.pushFilterUp

import scala.collection.Seq


object exactWithoutAdaptation extends QueryEngine_Abs("NoApp") {
  override def ReadNextQueries(query: String, ip: String, epoch: Long, queryIndex: Int): Seq[String] = null

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def main(args: Array[String]): Unit = {
    readConfiguration(args)
    val costModel = new ExactCostModel(sparkSession)
    loadTables(sparkSession)
    val queries = loadWorkloadWithIP("skyServer", sparkSession)
    // sparkSession.sql("select  count(*) as count from photoobj p ").show(100000)
    //  throw new Exception
    execute(costModel)
    printReport()
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