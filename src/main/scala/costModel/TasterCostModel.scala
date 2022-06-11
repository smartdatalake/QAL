package costModel


import costModel.bestSetSelector.CELF
import java.io.File

import scala.reflect.io.Directory
import definition.Paths._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.ReturnAnswer

import scala.collection.mutable.ListBuffer
import scala.collection.{Seq, mutable}

class TasterCostModel(sparkSession: SparkSession, justAPP: Boolean, isAdaptive: Boolean) extends CostModelAbs() {

  var currentSubQueriesAPP: Seq[Seq[SparkPlan]] = null // ( sub1(app1,app2) , sub2(app1,app2) )
  override val setSelectionStrategy = new CELF(this)
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def calRewardOfGain(app: SparkPlan, windowSize: Int): Double = {
    val synopsesOfApp = extractSynopses(app).map(y => (y.toString(), synopsesSize.getOrElse(y.toString(), costOfAppPlan(y, getWRSynopsesSize.toSeq)._1)))
    if (synopsesOfApp.size == 0)
      return 1.0
    if (synopsesOfApp.reduce((a, b) => ("", a._2 + b._2))._2 > maxSpace) {
      return -1
    }
    if (past.size == 0)
      return 0.0
    if (synopsesOfApp.reduce((a, b) => ("", a._2 + b._2))._2 > maxSpace) {
      //synopsesOfApp.foreach(println)
      //println("-----")
      return Double.MinValue
    }
    past.takeRight(windowSize).map(subQueries => subQueries.map(pps => {
      pps.map(costOfExact).minBy(_._2)._2 - pps.map(pp => costOfAppWithFixedSynopses(pp, synopsesOfApp)).minBy(_._2)._2
    }).reduce(_ + _)).reduce(_ + _)
  }

  override def suggest(): Seq[SparkPlan] = {
    currentSubQueriesAPP.map(subQueryAPPs => {
      subQueryAPPs.map(pp => {
        (pp, calRewardOfGain(pp, (windowSize)))
      }).maxBy(_._2)._1
    })
  }

  override def addQuery(query: String, f: Seq[String],futureProjectList:ListBuffer[String]): Unit = {
    future = f.map(query => getAggSubQueries(sparkSession.sqlContext.sql(query).queryExecution.analyzed).map(subQuery => {
      updateAttributeName(subQuery, new mutable.HashMap[String, Int]())
      val joins = enumerateRawPlanWithJoin(subQuery)
      val logicalPlans = joins.map(x => sparkSession.sessionState.optimizer.execute(x))
      logicalPlans.flatMap(x => sparkSession.sessionState.planner.plan(ReturnAnswer(x)))
    }))
    currentSubQueriesAPP = getAggSubQueries(sparkSession.sqlContext.sql(query).queryExecution.analyzed).map(subQuery => {
      updateAttributeName(subQuery, new mutable.HashMap[String, Int]())
      val joins = enumerateRawPlanWithJoin(subQuery)
      val logicalPlans = joins.map(x => sparkSession.sessionState.optimizer.execute(x))
      val apps = logicalPlans.flatMap(x => sparkSession.sessionState.planner.plan(ReturnAnswer(x)))
      // println(apps)
      val temp = sparkSession.experimental.extraOptimizations
      sparkSession.experimental.extraOptimizations = Seq()
      val exact = sparkSession.sessionState.planner.plan(ReturnAnswer(sparkSession.sessionState.optimizer.execute(subQuery)))
      sparkSession.experimental.extraOptimizations = temp
      //    if (justAPP)
      apps
      //   else
      //     apps ++ exact
    })
    past.+=(currentSubQueriesAPP)
  }

  override def updateWarehouse(): Unit = {
    val (keep, remove) = setSelectionStrategy.decide()
    if (remove.size == 0)
      return
    //keep.foreach(println)
    remove.foreach(x => {
      val parquetName = SynopsesToParquetName.getOrElse(x, "null")
      Directory(new File(pathToSaveSynopses + parquetName + ".obj")).deleteRecursively()
      println("removed" + ParquetNameToSynopses(parquetName) + "  " + warehouseParquetNameToSize(parquetName))
      warehouseParquetNameToSize.remove(parquetName)
      warehouseParquetNameToRow.remove(parquetName)
      SynopsesToParquetName.remove(x)
      ParquetNameToSynopses.remove(parquetName)
      sampleToOutput.remove(parquetName)
      parquetNameToHeader.remove(parquetName)
      lastUsedOfParquetSample.remove(parquetName)
      numberOfRemovedSynopses += 1
    })
  }

  def UpdateWindowHorizon() = {
    println("WWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWW" + windowSize.toString)
    val synopsesForWm = GetBestSynopses((windowSize * (1 - alpha)).toInt)
    val synopsesForW = GetBestSynopses(windowSize)
    val synopsesForWp = GetBestSynopses((windowSize * (1 + alpha)).toInt)
    val timeWm = calMinExecutionTimeBetweenTwoInvocations(synopsesForWm)
    val timeW = calMinExecutionTimeBetweenTwoInvocations(synopsesForW)
    val timeWp = calMinExecutionTimeBetweenTwoInvocations(synopsesForWp)
    if (timeWm < timeW && timeWm < timeWp)
      (windowSize * (1 - alpha)).toInt
    else if (timeWp < timeW && timeWp < timeWm)
      (windowSize * (1 + alpha)).toInt
    else
      windowSize
  }

  override def getFutureAPP(): Seq[Seq[Seq[SparkPlan]]] = future.take(windowSize) //Seq(currentSubQueriesAPP) ++ future.takeRight(windowSize - 1).reverse

  override def getFutureSize(): Long = future.take(windowSize).size


}

/*def estimateSizeOf(pp: SparkPlan): Long = pp match {
  case l@RDDScanExec(a, b, s, d, g) =>
    val size: Long = definition.Paths.mapRDDScanRowCNT.getOrElse(getTableColumnsName(l.output).mkString(";").toLowerCase, -1)
    if (size == -1)
      throw new Exception("The size does not exist: " + l.toString())
    size
  case ProjectExec(projectList, child) =>
    ((projectList.size / child.output.size.toDouble) * estimateSizeOf(child)).toLong
  case s: SampleExec =>
    (s.fraction * estimateSizeOf(s.child)).toLong
  case a: BinaryExecNode =>
    (a.children.map(estimateSizeOf).reduce(_ * _) * definition.Paths.fractionInitialize).toLong
  case a: UnaryExecNode =>
    estimateSizeOf(a.child)
  case _ =>
    throw new Exception("No size is defined for the node")
}*/