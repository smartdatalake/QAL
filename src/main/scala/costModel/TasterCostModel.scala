package costModel

import java.io.File

import definition.Paths._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.ReturnAnswer
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.mutable.ListBuffer
import scala.collection.{Seq, mutable}
import scala.reflect.io.Directory

class TasterCostModel(sparkSession: SparkSession, justAPP: Boolean, isAdaptive: Boolean) extends CostModelAbs() {

  //val futureQueries = new mutable.Queue[String]()
  val step = 10
  var past = new ListBuffer[Seq[Seq[SparkPlan]]]()
  var future = new mutable.Queue[Seq[Seq[SparkPlan]]]() // ( q1( sub1(app1,app2) , sub2(app1,app2) ),  q2( sub1(app1,app2) , sub2(app1,app2) ) )
  var currentSubQueriesAPP: Seq[Seq[SparkPlan]] = null // ( sub1(app1,app2) , sub2(app1,app2) )
  override val setSelectionStrategy = new CELF(this)
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /*
    def calRewardOf(A: Seq[(String, Long)]): Double =
      if (currentSubQueriesAPP == null && future.size == 0)
        throw new Exception("Invalid future")
      else if (future.size == 0)
        getCheapestExactCostOf(Seq(currentSubQueriesAPP)) - currentSubQueriesAPP.map(subQueryAPPs => subQueryAPPs.map(APP => costOfAppPlan(APP, A)._2).min).reduce(_ + _)
      else
        getCheapestExactCostOf(future ++ Seq(currentSubQueriesAPP)) - (future ++ Seq(currentSubQueriesAPP)).map(perSubqueryPhysicalPlan => perSubqueryPhysicalPlan.map(subQueryAPPs => subQueryAPPs.map(APP => costOfAppPlan(APP, A)._2).min).reduce(_ + _)).reduce(_ + _)

    // cheapestExact(F+Can_app)- { Exec(Can_app,WR) + Exec_min( F , S(Can_app) ) }

    def calRewardOf22222(app: SparkPlan): Double = {
      if (future.size == 0)
        getCheapestExactCostOf(app) - (costOfAppPlan(app, getWRSynopsesSize.toSeq)._2 + 0)
      else {
        val synopsesOfApp = extractSynopses(app).map(y => (y.toString(), costOfAppPlan(y, getWRSynopsesSize.toSeq)._1))
        getCheapestExactCostOf(future, app) - (costOfAppPlan(app, getWRSynopsesSize.toSeq)._2 + future.map(query => query.map(subQuery => subQuery.map(APP => costOfAppPlan(APP, synopsesOfApp)._2).min).reduce(_ + _)).reduce(_ + _))
      }
    }

    def calRewardOf3(app: SparkPlan, windowSize: Int): Double = {
      if (future.size == 0)
        (costOfAppPlan(app, getWRSynopsesSize.toSeq)._2 + 0)
      else {
        val synopsesOfApp = extractSynopses(app).map(y => (y.toString(), costOfAppPlan(y, getWRSynopsesSize.toSeq)._1))
        (costOfAppPlan(app, getWRSynopsesSize.toSeq)._2 + future.takeRight(windowSize).map(query => query.map(subQuery => subQuery.map(APP => costOfAppPlan(APP, synopsesOfApp)._2).min).reduce(_ + _)).reduce(_ + _))
      }
    }*/


  def calRewardOfGain(app: SparkPlan, windowSize: Int): Double = {
    val synopsesOfApp = extractSynopses(app).map(y => (y.toString(), synopsesSize.getOrElse(y.toString(),costOfAppPlan(y, getWRSynopsesSize.toSeq)._1)))
    if (synopsesOfApp.size == 0)
      return 1.0
    if (synopsesOfApp.reduce((a, b) => ("", a._2 + b._2))._2 > maxSpace) {
    //  synopsesOfApp.foreach(println)
      return -1
    }
    if (past.size == 0)
      return 0.0
    past.takeRight(windowSize).map(subQueries => subQueries.map(pps => {
      pps.map(costOfExact).minBy(_._2)._2 - pps.map(pp => costOfAppWithFixedSynopses(pp, synopsesOfApp)).minBy(_._2)._2
    }).reduce(_ + _)).reduce(_ + _)
  }


  override def suggest(): Seq[SparkPlan] = {
    if (isAdaptive && past.size > 0 && past.size % step == 0)
      updateWindowSize()
    return currentSubQueriesAPP.map(subQueryAPPs => {
      subQueryAPPs.map(pp => {
    //    println(pp)
    //    println(calRewardOfGain(pp, (windowSize)))
        (pp, calRewardOfGain(pp, (windowSize)))
      }).maxBy(_._2)._1
    })

    /* val wM = currentSubQueriesAPP.map(subQueryAPPs => {
       subQueryAPPs.map(pp => {
         (pp, calRewardOfGain(pp, (windowSize * (1 - alpha)).toInt)) /*calRewardOf(extractSynopses(pp).map(y => (y.toString(), costOfAppPlan(y, Seq())._1))))*/
       }).maxBy(_._2)
     })

     val w = currentSubQueriesAPP.map(subQueryAPPs => {
       subQueryAPPs.map(pp => {
         (pp, calRewardOfGain(pp, windowSize)) /*calRewardOf(extractSynopses(pp).map(y => (y.toString(), costOfAppPlan(y, Seq())._1))))*/
       }).maxBy(_._2)
     })

     val wP = currentSubQueriesAPP.map(subQueryAPPs => {
       subQueryAPPs.map(pp => {
         (pp, calRewardOfGain(pp, (windowSize * (1 + alpha)).toInt)) /*calRewardOf(extractSynopses(pp).map(y => (y.toString(), costOfAppPlan(y, Seq())._1))))*/
       }).maxBy(_._2)
     })

     if (wM(0)._2 > w(0)._2 && wM(0)._2 > wP(0)._2) {
       windowSize = (windowSize * (1 - alpha)).toInt
       return wM.map(_._1)
     }

     if (wP(0)._2 > w(0)._2 && wP(0)._2 > wM(0)._2) {
       windowSize = (windowSize * (1 + alpha)).toInt
       return wP.map(_._1)
     }
     w.map(_._1)*/
  }

  /*  override def suggest(): Seq[SparkPlan] = {
      //println("--------------------------------------------------------------------------")
      //currentSubQueriesAPP.map(subQueryAPPs => subQueryAPPs.foreach(pp => println(toStringTree(pp))))
      //println("--------------------------------------------------------------------------")
      currentSubQueriesAPP.map(subQueryAPPs => {
        subQueryAPPs.map(pp => {
          // println(toStringTree(pp))
          //  println(calRewardOf(extractSynopses(pp).map(y => (y.toString(), costOfAppPlan(y, Seq())._2))))
          (pp, calRewardOf(extractSynopses(pp).map(y => (y.toString(), costOfAppPlan(y, Seq())._1))))
        }).maxBy(_._2)._1
      })
    }*/

  override def addQuery(query: String, ip: String, epoch: Long, f: Seq[String]): Unit = {
    // if (currentSubQueriesAPP != null) future.enqueue(currentSubQueriesAPP)
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
    past.+=(getAggSubQueries(sparkSession.sqlContext.sql(query).queryExecution.analyzed).map(subQuery => {
      updateAttributeName(subQuery, new mutable.HashMap[String, Int]())
      val joins = enumerateRawPlanWithJoin(subQuery)
      val logicalPlans = joins.map(x => sparkSession.sessionState.optimizer.execute(x))
      logicalPlans.flatMap(x => sparkSession.sessionState.planner.plan(ReturnAnswer(x)))
    }))
  }

  override def updateWarehouse(): Unit = {
    val (keep, remove) = setSelectionStrategy.decide()
    if (remove.size == 0)
      return
    keep.foreach(println)
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

  def updateWindowSize() = {
    println("WWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWW" + windowSize.toString)
    val synopsesForWm = GetBestSynopses((windowSize * (1 - alpha)).toInt)
    val synopsesForW = GetBestSynopses(windowSize)
    val synopsesForWp = GetBestSynopses((windowSize * (1 + alpha)).toInt)
    val timeWm = calMinExecutionTimeBetweenTwoInvocations(synopsesForWm)
    val timeW = calMinExecutionTimeBetweenTwoInvocations(synopsesForW)
    val timeWp = calMinExecutionTimeBetweenTwoInvocations(synopsesForWp)
    if (timeWm < timeW && timeWm < timeWp)
      windowSize = (windowSize * (1 - alpha)).toInt
    else if (timeWp < timeW && timeWp < timeWm)
      windowSize = (windowSize * (1 + alpha)).toInt
    println("WWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWW" + windowSize.toString)
  }

  def calMinExecutionTimeBetweenTwoInvocations(A: Seq[(String, Long)]): Long = {
    past.takeRight(step).map(subQueries => subQueries.map(pps => {
      pps.map(pp => costOfAppWithFixedSynopses(pp, A)).minBy(_._2)._2
    }).reduce(_ + _)).reduce(_ + _)
  }

  def GetBestSynopses(w: Int): Seq[(String, Long)] = {
    val A: ListBuffer[(String, Long)] = ListBuffer[(String, Long)]()
    val VMinusA = new mutable.HashMap[String, Long]()
    past.takeRight(w).flatMap(x => x.flatMap(y => y.flatMap(z => extractSynopses(z)))).map(y => (y.toString(), costOfAppPlan(y, Seq())._1)).foreach(x => VMinusA.put(x._1, x._2))
    var sizeOfA: Long = 0
    if (VMinusA.reduce((a, b) => (null, a._2 + b._2))._2 <= maxSpace)
      return VMinusA.toSeq
    while (VMinusA.find(s => s._2 + sizeOfA <= maxSpace).isDefined) {
      val eachSynopsisMarginalRewardForCurrentA = VMinusA.map(synopsis // { ( (sample1,cost) ,marginalGain) }
      => (synopsis,
          past.takeRight(w).map(subQueries => subQueries.map(pps => {
            pps.map(pp => costOfAppWithFixedSynopses(pp, A)).minBy(_._2)._2 - pps.map(pp => costOfAppWithFixedSynopses(pp, A ++ Seq(synopsis))).minBy(_._2)._2
          }).reduce(_ + _)).reduce(_ + _)
        )).toList.sortBy(_._2)(Ordering[Long].reverse)
      val bestSynopsisOption = eachSynopsisMarginalRewardForCurrentA.find(x => x._1._2 + sizeOfA <= maxSpace)
      val best = bestSynopsisOption.get
      A.+=(best._1)
      sizeOfA += best._1._2
      VMinusA.remove(best._1._1)
    }
    return A
  }

  override def getFutureAPP(): Seq[Seq[Seq[SparkPlan]]] = past.takeRight(windowSize) //Seq(currentSubQueriesAPP) ++ future.takeRight(windowSize - 1).reverse

  override def getFutureSize(): Long = past.takeRight(windowSize).size


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