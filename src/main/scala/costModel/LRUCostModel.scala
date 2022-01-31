package costModel

import java.io.File

import definition.Paths._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.ReturnAnswer
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.{Seq, mutable}
import scala.reflect.io.Directory

class LRUCostModel(sparkSession: SparkSession) extends CostModelAbs {
  val future = null // ( q1( sub1(app1,app2) , sub2(app1,app2) ),  q2( sub1(app1,app2) , sub2(app1,app2) ) )
  var currentSubQueriesAPP: Seq[Seq[SparkPlan]] = null // ( sub1(app1,app2) , sub2(app1,app2) )
  override val setSelectionStrategy = new LRU(this)

  override def getFutureSize(): Long = 0

  override def getFutureAPP(): Seq[Seq[Seq[SparkPlan]]] = Seq()

  def calRewardOf(A: Seq[(String, Long)]): Double =
    getCheapestExactCostOf(Seq(currentSubQueriesAPP)) - currentSubQueriesAPP.map(subQueryAPPs => subQueryAPPs.map(APP => costOfAppPlan(APP, A)._2).min).reduce(_ + _)


  def calRewardOf22222(app: SparkPlan): Double = {
    val synopsesOfApp = extractSynopses(app).map(y => (y.toString(), synopsesSize.getOrElse(y.toString(), costOfAppPlan(y, getWRSynopsesSize.toSeq)._1)))
    if (synopsesOfApp.reduce((a, b) => ("", a._2 + b._2))._2 > maxSpace)
      return Double.MaxValue
    (costOfAppWithFixedSynopses(app, getWRSynopsesSize.toSeq)._2)
  }

  // Exec(Can_app,WR)

  override def suggest(): Seq[SparkPlan] = {
    //println("--------------------------------------------------------------------------")
    //currentSubQueriesAPP.map(subQueryAPPs => subQueryAPPs.foreach(pp => println(toStringTree(pp))))
    //println("--------------------------------------------------------------------------")
    currentSubQueriesAPP.map(subQueryAPPs => {
      subQueryAPPs.map(pp => {
        //  println(toStringTree(pp))
        //    println(calRewardOf22222(pp))
        (pp, calRewardOf22222(pp)) /*calRewardOf(extractSynopses(pp).map(y => (y.toString(), costOfAppPlan(y, Seq())._1))))*/
      }).minBy(_._2)._1
    })
  }


  /* override def suggest(): Seq[SparkPlan] = {
     val warehouseSynopsesAndSize = warehouseParquetNameToSize.map(x => (ParquetNameToSynopses(x._1), x._2)).toSeq // { (sample1,size) }
     currentSubQueriesAPP.map(subQueryAPPs => {
       subQueryAPPs.map(pp => {
         (pp, calRewardOf(warehouseSynopsesAndSize))
       }).maxBy(_._2)._1
     })
   }*/

  override def addQuery(query: String, ip: String, epoch: Long, f: Seq[String] = null): Unit = {
    currentSubQueriesAPP = getAggSubQueries(sparkSession.sqlContext.sql(query).queryExecution.analyzed).map(subQuery => {
      updateAttributeName(subQuery, new mutable.HashMap[String, Int]())
      val joins = enumerateRawPlanWithJoin(subQuery)
      val logicalPlans = joins.map(x => sparkSession.sessionState.optimizer.execute(x))
      logicalPlans.flatMap(x => sparkSession.sessionState.planner.plan(ReturnAnswer(x)))
    })
  }

  override def updateWarehouse(): Unit = {
    val (keep, remove) = setSelectionStrategy.decide()
    if (remove.size == 0)
      return
    remove.foreach(x => {
      val parquetName = SynopsesToParquetName.getOrElse(x, "null")
      Directory(new File(pathToSaveSynopses + parquetName + ".obj")).deleteRecursively()
      println("removed" + ParquetNameToSynopses(parquetName))
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
}
