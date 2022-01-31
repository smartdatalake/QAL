package costModel

import java.io.File

import definition.Paths._
import operators.physical.{SampleExec, UniformSampleExec2, UniversalSampleExec2}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.ReturnAnswer
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}

import scala.collection.mutable.ListBuffer
import scala.collection.{Seq, mutable}
import scala.reflect.io.Directory

class StaticCostModel(sparkSession: SparkSession, justAPP: Boolean, isExtended: Boolean, isAdaptive: Boolean) extends CostModelAbs {
  override val setSelectionStrategy: BestSetSelectorAbs = new CELF(this)
  var future: Seq[Seq[Seq[SparkPlan]]] = null // ( q1( sub1(app1,app2) , sub2(app1,app2) ),  q2( sub1(app1,app2) , sub2(app1,app2) ) )
  var currentSubQueriesAPP: Seq[Seq[SparkPlan]] = null // ( sub1(app1,app2) , sub2(app1,app2) )
  var past = new ListBuffer[Seq[Seq[SparkPlan]]]()
  val step = 10

  /*
    def calRewardOf22222(app: SparkPlan): Double = {

      val synopsesOfApp = extractSynopses(app).map(y => (y.toString(), costOfAppPlan(y, getWRSynopsesSize.toSeq)._1))
      getCheapestExactCostOf(future, app) - (costOfAppPlan(app, getWRSynopsesSize.toSeq)._2 + future.map(query => query.map(subQuery => subQuery.map(APP => costOfAppPlan(APP, synopsesOfApp)._2).min).reduce(_ + _)).reduce(_ + _))
    }

    def calRewardOf3(app: SparkPlan): Double = {

      val synopsesOfApp = extractSynopses(app).map(y => (y.toString(), costOfAppPlan(y, getWRSynopsesSize.toSeq)._1))
      //println((costOfAppPlan(app, getWRSynopsesSize.toSeq)._2))
      (costOfAppPlan(app, getWRSynopsesSize.toSeq)._2 + future.map(query => query.map(subQuery => subQuery.map(APP => costOfAppPlan(APP, synopsesOfApp)._2).min).reduce(_ + _)).reduce(_ + _))
    }*/

  override def getFutureProjectList(): Seq[String] = future.take(windowSize).flatMap(
    subQueries => subQueries.flatMap(subQueryPPs => subQueryPPs(0).flatMap(node => {
      if (node.isInstanceOf[ProjectExec])
        node.output
      else if (node.isInstanceOf[SortMergeJoinExec])
        node.asInstanceOf[SortMergeJoinExec].leftKeys.find(_.isInstanceOf[Attribute]).get.map(l => l.asInstanceOf[Attribute]) ++ node.asInstanceOf[SortMergeJoinExec].rightKeys.find(_.isInstanceOf[Attribute]).get.map(l => l.asInstanceOf[Attribute])
      else Seq()
    }))).map(getAttNameOfAtt).distinct

  def getJoinKeys(): Seq[AttributeReference] = future.take(windowSize).flatMap(
    subQueries => subQueries.flatMap(subQueryPPs => subQueryPPs(0).flatMap(node => {
      if (node.isInstanceOf[ShuffledHashJoinExec])
        Seq(definition.Paths.getAttRefFromExp(node.asInstanceOf[ShuffledHashJoinExec].leftKeys(0))(0), definition.Paths.getAttRefFromExp(node.asInstanceOf[ShuffledHashJoinExec].rightKeys(0))(0))
      else if (node.isInstanceOf[BroadcastHashJoinExec])
        Seq(definition.Paths.getAttRefFromExp(node.asInstanceOf[BroadcastHashJoinExec].leftKeys(0))(0), definition.Paths.getAttRefFromExp(node.asInstanceOf[BroadcastHashJoinExec].rightKeys(0))(0))
      else if (node.isInstanceOf[SortMergeJoinExec])
        Seq(definition.Paths.getAttRefFromExp(node.asInstanceOf[SortMergeJoinExec].leftKeys(0))(0), definition.Paths.getAttRefFromExp(node.asInstanceOf[SortMergeJoinExec].rightKeys(0))(0))
      else Seq()
    }))).distinct

  def calRewardOf3(app: SparkPlan): Double = {
    if (future.size == 0)
      return costOfAppPlan(app, getWRSynopsesSize.toSeq)._2
    val synopsesOfApp = extractSynopses(app).map(y => (y.toString(), synopsesSize.getOrElse(y.toString(), costOfAppPlan(y, getWRSynopsesSize.toSeq)._1)))
    if (synopsesOfApp.size == 0)
      return costOfAppPlan(app, getWRSynopsesSize.toSeq)._2 + future.take(windowSize).map(query => query.map(subQuery => subQuery.map(APP => costOfExact(APP)._2).min).reduce(_ + _)).reduce(_ + _)
    if (synopsesOfApp.reduce((a, b) => ("", a._2 + b._2))._2 > maxSpace) {
      //synopsesOfApp.foreach(println)
      //println("-----")
      return Double.MaxValue
    }

    (costOfAppPlan(app, getWRSynopsesSize.toSeq)._2 + (future.take(windowSize).map(query => query.map(subQuery => subQuery.map(APP => costOfAppWithFixedSynopses(APP, synopsesOfApp)._2).min).reduce(_ + _)).reduce(_ + _)))
  }


  // cheapestExact(P+Can_app)- { Exec(Can_app,WR) + Exec_min( P , S(Can_app) ) }

  override def suggest(): Seq[SparkPlan] = {
    //println("--------------------------------------------------------------------------")
    //currentSubQueriesAPP.map(subQueryAPPs => subQueryAPPs.foreach(pp => println(toStringTree(pp))))
    //println("--------------------------------------------------------------------------")
    // prediction.foreach(println)
    val warehouse = warehouseParquetNameToSize.map(x => ParquetNameToSynopses(x._1)).toSeq
    val temp = currentSubQueriesAPP.map(apps => apps.find(app => AreCovered(extractSynopses(app).map(_.asInstanceOf[SampleExec]), warehouse)))
    if (temp.map(_.isDefined).reduce(_ && _))
      return temp.map(_.get)
    (currentSubQueriesAPP).map(subQueryAPPs => {
      //println("---")
      subQueryAPPs.map(pp => {
        //     println(toStringTree(pp))
        //      println(calRewardOf3(pp))
        (pp, calRewardOf3(pp)) /*calRewardOf(extractSynopses(pp).map(y => (y.toString(), costOfAppPlan(y, Seq())._1))))*/
      }).minBy(_._2)._1
    })
  }


  /*  def calRewardOfGain(app: SparkPlan, windowSize: Int): Double = {
      val synopsesOfApp = extractSynopses(app).map(y => (y.toString(), costOfAppPlan(y, getWRSynopsesSize.toSeq)._1))
      if (synopsesOfApp.size == 0)
        return 1.0
      if (synopsesOfApp.reduce((a, b) => ("", a._2 + b._2))._2 > maxSpace)
        return -1
      future.take(windowSize).map(subQueries => subQueries.map(pps => {
        pps.map(costOfExact).minBy(_._2)._2 - pps.map(pp => costOfAppWithFixedSynopses(pp, synopsesOfApp)).minBy(_._2)._2
      }).reduce(_ + _)).reduce(_ + _)
    }

    // cheapestExact(F+Can_app)- { Exec(Can_app,WR) + Exec_min( F , S(Can_app) ) }

    override def suggest(): Seq[SparkPlan] = {
      //println("--------------------------------------------------------------------------")
      //currentSubQueriesAPP.map(subQueryAPPs => subQueryAPPs.foreach(pp => println(toStringTree(pp))))
      //println("--------------------------------------------------------------------------")
      if (isAdaptive && past.size > 0 && past.size % step == 0)
        updateWindowSize()
      currentSubQueriesAPP.map(subQueryAPPs => {
        subQueryAPPs.map(pp => {
          //    println(toStringTree(pp))
          //     println(calRewardOfGain(pp, windowSize))
          (pp, calRewardOfGain(pp, windowSize)) /*calRewardOf(extractSynopses(pp).map(y => (y.toString(), costOfAppPlan(y, Seq())._1))))*/
        }).maxBy(_._2)._1
      })
    }*/


  override def addQuery(query: String, ip: String, epoch: Long, f: Seq[String]): Unit = {
    future = f.map(query => getAggSubQueries(sparkSession.sqlContext.sql(query).queryExecution.analyzed).map(subQuery => {
      updateAttributeName(subQuery, new mutable.HashMap[String, Int]())
      val joins = enumerateRawPlanWithJoin(subQuery)
      val logicalPlans = joins.map(x => sparkSession.sessionState.optimizer.execute(x))
      logicalPlans.flatMap(x => sparkSession.sessionState.planner.plan(ReturnAnswer(x)))
    }))
    val j = getJoinKeys()
    currentSubQueriesAPP = getAggSubQueries(sparkSession.sqlContext.sql(query).queryExecution.analyzed).map(subQuery => {
      updateAttributeName(subQuery, new mutable.HashMap[String, Int]())
      /*val joins = enumerateRawPlanWithJoin(subQuery)
      val logicalPlans = joins.map(x => sparkSession.sessionState.optimizer.execute(x))
      logicalPlans.flatMap(x => sparkSession.sessionState.planner.plan(ReturnAnswer(x)))*/
      val joins = enumerateRawPlanWithJoin(subQuery)
      val logicalPlans = joins.map(x => sparkSession.sessionState.optimizer.execute(x))
      val apps = logicalPlans.flatMap(x => sparkSession.sessionState.planner.plan(ReturnAnswer(x)).map(x => ExtendProject(x, getFutureProjectList)).flatMap(p => {
        if (p.find(x => x.isInstanceOf[UniformSampleExec2]).isDefined) {
          val pp = p.find(x => x.isInstanceOf[UniformSampleExec2]).get.asInstanceOf[UniformSampleExec2]
          if (pp.joins == "_") {
            val ttt = pp.output.find(x => j.find(y => x.toAttribute.toString() == y.toAttribute.toString()).isDefined)
            if (ttt.isDefined)
              Seq(p, p.transform({
                case u: UniformSampleExec2 => new UniversalSampleExec2(pp.functions, pp.confidence, pp.error, pp.seed, Seq(ttt.get.asInstanceOf[AttributeReference]), pp.children(0))
              }))
            else
              Seq(p)
          }
          else
            Seq(p)
        }
        else
          Seq(p)
      })) ++ logicalPlans.flatMap(x => sparkSession.sessionState.planner.plan(ReturnAnswer(x)))
      val temp = sparkSession.experimental.extraOptimizations
      sparkSession.experimental.extraOptimizations = Seq()
      val exact = sparkSession.sessionState.planner.plan(ReturnAnswer(sparkSession.sessionState.optimizer.execute(subQuery)))
      sparkSession.experimental.extraOptimizations = temp
      if (justAPP)
        apps
      else
        apps ++ exact
    }

    )
    past.+=(getAggSubQueries(sparkSession.sqlContext.sql(query).queryExecution.analyzed).map(subQuery => {
      updateAttributeName(subQuery, new mutable.HashMap[String, Int]())
      val joins = enumerateRawPlanWithJoin(subQuery)
      val logicalPlans = joins.map(x => sparkSession.sessionState.optimizer.execute(x))
      logicalPlans.flatMap(x => sparkSession.sessionState.planner.plan(ReturnAnswer(x)))
    }))
    //getFutureProjectList().foreach(println)
  }

  override def getFutureAPP(): Seq[Seq[Seq[SparkPlan]]] = future.take(windowSize)

  override def getFutureSize(): Long = future.size


  /*override def suggest(): Seq[SparkPlan] = {
    //println("--------------------------------------------------------------------------")
    //currentSubQueriesAPP.map(subQueryAPPs => subQueryAPPs.foreach(pp => println(toStringTree(pp))))
    //println("--------------------------------------------------------------------------")
    currentSubQueriesAPP.map(subQueryAPPs => {
      subQueryAPPs.map(pp => {
        println(toStringTree(pp))
        println(extractSynopses(pp).map(y => (y.toString(), costOfAppPlan(y, Seq())._1)))
        //  println(calRewardOf(extractSynopses(pp).map(y => (y.toString(), costOfAppPlan(y, Seq())._1))))
        (pp, calRewardOf(extractSynopses(pp).map(y => (y.toString(), costOfAppPlan(y, Seq())._1))))
      }).maxBy(_._2)._1
    })
  }*/


  override def updateWarehouse(): Unit = {
    val (keep, remove) = setSelectionStrategy.decide()
    if (remove.size == 0)
      return
    // if(parquetName.equals("null")){
    //   println(remove)
    //  println(keep)
    //   warehouseParquetNameToSize.foreach(println)
    //   println(SynopsesToParquetName)
    //   ParquetNameToSynopses.foreach(println)
    //   println(sampleToOutput)
    //  println(parquetNameToHeader)
    //  }
    keep.foreach(println)
    remove.foreach(x => {
      val parquetName = SynopsesToParquetName.getOrElse(x, "null")

      Directory(new File(pathToSaveSynopses + parquetName + ".obj")).deleteRecursively()
      println("removed" + ParquetNameToSynopses(parquetName) + "  " + warehouseParquetNameToSize(parquetName))
      warehouseParquetNameToRow.remove(parquetName)
      warehouseParquetNameToSize.remove(parquetName)
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
}
