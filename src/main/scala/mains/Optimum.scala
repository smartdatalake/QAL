package mains

import java.io.File

import costModel.StaticCostModel
import definition.Paths._
import operators.physical.{SampleExec, UniformSampleExec2, UniversalSampleExec2}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.ReturnAnswer
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import rules.logical.{ApproximateInjector, pushFilterUp}
import rules.physical.SampleTransformationMultiple

import scala.collection.{Seq, mutable}
import scala.reflect.io.Directory

object Optimum extends QueryEngine_Abs("Utopia") {
  var costModel: StaticCostModel = null
  val synopsesPool: mutable.HashMap[Int, (String, Long)] = new mutable.HashMap[Int, (String, Long)]()
  val reverseSynopsesPool: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()

  var queries: Seq[(String, String, Long)] = null

  def main(args: Array[String]): Unit = {
    readConfiguration(args)
    loadTables(sparkSession)
    costModel = new StaticCostModel(sparkSession, justAPP = justAPP, isExtended = isExtended, isAdaptive = isAdaptive)
    val remain = new mutable.Queue[(String, Long)]()
    val partialRemain = new mutable.Queue[(String, Long)]()

    val workload = loadWorkloadWithIP("skyServer", sparkSession).toSeq
    mapRDDScanRowCNT = readRDDScanRowCNT(sparkSession)
    sparkSession.experimental.extraStrategies = Seq(SampleTransformationMultiple)
    sparkSession.experimental.extraOptimizations = Seq(new ApproximateInjector(confidence, error, seed), new pushFilterUp())
    workload.foreach(query => {
      val lp = sparkSession.sqlContext.sql(query._1).queryExecution.analyzed
      updateAttributeName(lp, new mutable.HashMap[String, Int]())
    })
    var w = ""
    var totalTime: Long = 0
    var i = 0
    var counter = 1
    for (queries <- workload.sliding(6)) {
      //queries = loadWorkloadWithIP("skyServer", sparkSession).toSeq
      this.queries = queries
      println(counter  + "qqqqqqqqqqqqqqqqqqqqqqqq" + queries(0)._1)
      counter += 1
      val j = getJoinKeys()
      val futureColumn = getFutureProjectList()
      val Q = queries.map(query => {
        val lp = sparkSession.sqlContext.sql(query._1).queryExecution.analyzed
        updateAttributeName(lp, new mutable.HashMap[String, Int]())
        val joins = costModel.enumerateRawPlanWithJoin(lp)
        val logicalPlans = joins.map(x => sparkSession.sessionState.optimizer.execute(x))
        val apps = (logicalPlans.flatMap(x => sparkSession.sessionState.planner.plan(ReturnAnswer(x)).map(x => ExtendProject(x, futureColumn)).flatMap(p => {
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
        })) ++ logicalPlans.flatMap(x => sparkSession.sessionState.planner.plan(ReturnAnswer(x)))).distinct.map(x =>
          (x, extractSynopses(x).map(y => (y.asInstanceOf[SampleExec], synopsesSize.getOrElse(y.toString(), costModel.costOfAppPlan(y, costModel.getWRSynopsesSize.toSeq)._1)))))
        val temp = sparkSession.experimental.extraOptimizations
        sparkSession.experimental.extraOptimizations = Seq()
        val exact = sparkSession.sessionState.planner.plan(ReturnAnswer(sparkSession.sessionState.optimizer.execute(lp)))
        sparkSession.experimental.extraOptimizations = temp
        apps ++ exact.map(x => (x, Seq()))
      })
      Q.flatMap(x => x.flatMap(y => y._2)).map(x => (x._1.toString(), x._2)).distinct.foreach(x => {
        if (reverseSynopsesPool.get(x._1).isEmpty) {
          //println(i + "" + x)
          synopsesPool.put(i, x)
          reverseSynopsesPool.put(x._1, i)
          i += 1
        }
      })
      // Q.flatMap(x=>x.flatMap(y=>y._2.map(_._1).distinct).distinct).distinct.groupBy(_.toString()).map(x=>x._2(0)).foreach(x=>executeAndStoreSample(x, sparkSession))
      val end = queries.size - 1
      val Results = new mutable.Queue[(String, Long, String, Int)]()
      val Final = new mutable.Queue[(String, Long, String, Int)]()

      Results.enqueue(("", 0, w, 0))
      while (!Results.isEmpty) {
        val t = Results.dequeue()
        if (t._4 > end)
          Final.enqueue(t)
        //println(t)
        else {
          var c = 0
          for (p <- Q(t._4)) {
            if (AreCovered(p._2, t._3)) {
              val cost = t._2 + costModel.costOfAppPlan(p._1, (if (t._3 == "") Seq() else t._3.split(",").map(x => synopsesPool.get(x.toInt).get).toSeq))._2
              if (Results.find(r => r._4 == t._4 + 1 && r._2 <= cost && r._3.equals(t._3)).isEmpty)
                Results.enqueue((t._1 + ";exec(P" + t._4 + "," + c + ")"
                  , cost
                  , t._3
                  , t._4 + 1))
            }
            else if (p._2.reduce((a, b) => (null, a._2 + b._2))._2 < costModel.maxSpace) {
              val warehouse = if (t._3 == "") Seq() else t._3.split(",").map(x => (x, synopsesPool.get(x.toInt).get)).toSeq
              var temp = ""
              var size: Long = 0
              var added = ""
              for (s <- p._2) {
                val availableInWarehouse = warehouse.find(y => costModel.isMoreAccurate(s._1, y._2._1))
                if (availableInWarehouse.isDefined) {
                  temp += ("," + availableInWarehouse.get._1)
                  size += availableInWarehouse.get._2._2
                }
                else {
                  temp += ("," + reverseSynopsesPool.get(s._1.toString()).get)
                  added += ("," + reverseSynopsesPool.get(s._1.toString()).get)
                  size += s._2
                }
              }
              temp = temp.drop(1)
              added = added.drop(1)
              val wMS = warehouse.filter(x => temp.split(",").find(y => y == x._1).isEmpty)
              //val q = new mutable.Queue[(String, Long)]()
              //q.enqueue(("", 0))
              remain.clear()
              remain.enqueue(("", size))
              for (s <- wMS) {
                partialRemain.clear()
                for (p <- remain)
                  if (s._2._2 + p._2 <= costModel.maxSpace)
                    partialRemain.enqueue((p._1 + "," + s._1, p._2 + s._2._2))
                partialRemain.foreach(x => remain.enqueue(x))
                //val tt = q.dequeue()
                //if (tt._2 + s._2._2 + size < costModel.maxSpace)
                //   q.enqueue((tt._1 + "," + s._1, tt._2 + s._2._2))
                // q.enqueue(tt)
              }
              val x = remain.filter(x => x._1 != "").filter(x => wMS.find(y => (x._1.split(",").drop(1).find(z => z == y._1).isEmpty && y._2._2 + x._2 < costModel.maxSpace)).isEmpty)
              if (x.size == 0) {
                val removed = wMS.map(_._1).mkString(",")
                val cost = t._2 + costModel.costOfAppPlan(p._1, (if (t._3 == "") Seq() else t._3.split(",").map(x => synopsesPool.get(x.toInt).get).toSeq))._2
                if (Results.find(r => r._4 == t._4 + 1 && r._2 <= cost && r._3.equals(temp)).isEmpty) {
                  Results.enqueue((t._1 + (if (removed == "") "" else ";remove(" + removed + ")") + ";add(" + added + ");exec(P" + t._4 + "," + c + ")"
                    , cost
                    , temp
                    , t._4 + 1))
                  //  println(temp)
                }
              }
              else {
                for (i <- x) {
                  //  println(x.size)
                  val removed = wMS.filter(x => i._1.drop(1).split(",").find(y => y == x._1).isEmpty).map(_._1).mkString(",")
                  val cost = t._2 + costModel.costOfAppPlan(p._1, (if (t._3 == "") Seq() else t._3.split(",").map(x => synopsesPool.get(x.toInt).get).toSeq))._2
                  if (Results.find(r => r._4 == t._4 + 1 && r._2 <= cost && r._3.equals(temp)).isEmpty) {
                    Results.enqueue((t._1 + (if (removed == "") "" else ";remove(" + removed + ")") + ";add(" + added + ");exec(P" + t._4 + "," + c + ")"
                      , cost
                      , temp + "," + i._1.drop(1)
                      , t._4 + 1))
                    // println(temp + "," + i._1.drop(1))
                  }
                }
              }
            }
            c += 1
          }
        }
      }
      //Final.foreach(println)
      //Q.foreach(apps => apps.foreach(a => println(a._1)))

      val seq = Final.sortBy(_._2).toSeq(0)._1
      //println(seq)
      val actions = seq.substring(0, seq.indexOf(';', seq.indexOf("exec(P0,"))).split(";")
      //actions.foreach(println)
      var time = System.nanoTime()
      var outputOfQuery = ""
      for (a <- actions) {
        if (a.contains("exec")) {
          val i = a.substring(a.indexOf('P') + 1, a.indexOf(')')).split(",")(0).toInt
          val j = a.substring(a.indexOf('P') + 1, a.indexOf(')')).split(",")(1).toInt
          var cheapest = changeSynopsesWithScan(prepareForExecution(Q(i)(j)._1, sparkSession))

          executeAndStoreSample(cheapest, sparkSession)
          cheapest = changeSynopsesWithScan(cheapest)
          //println(cheapest)
          cheapest.executeCollectPublic().toList.sortBy(_.toString()).foreach(row => {
            //   println(row.toString())
            outputOfQuery += row.toString() + "\n"
          })
          actions
        }
        else if (a.contains("remove")) {
          val remove = a.substring(a.indexOf('(') + 1, a.indexOf(')')).split(",").map(x => synopsesPool.get(x.toInt).get._1)
          remove.foreach(x => {
            val parquetName = SynopsesToParquetName.getOrElse(x, "null")
            Directory(new File(pathToSaveSynopses + parquetName + ".obj")).deleteRecursively()
            println("removed" + ParquetNameToSynopses(parquetName) + "  ") //+ warehouseParquetNameToSize(parquetName))
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
      }

      w = SynopsesToParquetName.map(x => reverseSynopsesPool.get(x._1).get).mkString(",")
      val ttime = ((System.nanoTime() - time) )
      totalTime += ttime
      println(ttime/ 1000000000 + "," + totalTime/ 1000000000)
    }

  }

  def getFutureProjectList(): Seq[String] = queries.map(query => sparkSession.sqlContext.sql(query._1).queryExecution.executedPlan).flatMap(
    subQueryPPs => subQueryPPs.flatMap(node => {
      if (node.isInstanceOf[ProjectExec])
        node.output
      else if (node.isInstanceOf[SortMergeJoinExec])
        node.asInstanceOf[SortMergeJoinExec].leftKeys(0).find(_.isInstanceOf[Attribute]).get.map(l => l.asInstanceOf[Attribute]) ++ node.asInstanceOf[SortMergeJoinExec].rightKeys(0).find(_.isInstanceOf[Attribute]).map(l => l.asInstanceOf[Attribute])
      else Seq()
    })).map(getAttNameOfAtt).distinct

  def getJoinKeys(): Seq[AttributeReference] = queries.map(query => sparkSession.sqlContext.sql(query._1).queryExecution.executedPlan).flatMap(
    subQueryPPs => subQueryPPs(0).flatMap(node => {
      if (node.isInstanceOf[ShuffledHashJoinExec])
        Seq(definition.Paths.getAttRefFromExp(node.asInstanceOf[ShuffledHashJoinExec].leftKeys(0))(0), definition.Paths.getAttRefFromExp(node.asInstanceOf[ShuffledHashJoinExec].rightKeys(0))(0))
      else if (node.isInstanceOf[BroadcastHashJoinExec])
        Seq(definition.Paths.getAttRefFromExp(node.asInstanceOf[BroadcastHashJoinExec].leftKeys(0))(0), definition.Paths.getAttRefFromExp(node.asInstanceOf[BroadcastHashJoinExec].rightKeys(0))(0))
      else if (node.isInstanceOf[SortMergeJoinExec])
        Seq(definition.Paths.getAttRefFromExp(node.asInstanceOf[SortMergeJoinExec].leftKeys(0))(0), definition.Paths.getAttRefFromExp(node.asInstanceOf[SortMergeJoinExec].rightKeys(0))(0))
      else Seq()
    })).distinct

  def AreCovered(samples: Seq[(SampleExec, Long)], w: String): Boolean = {
    if (samples.size == 0) return true
    if (w == "") return false
    val warehouse = w.split(",").map(x => synopsesPool.get(x.toInt).get).toSeq
    samples.foreach(sample => {
      if (!warehouse.find(y => costModel.isMoreAccurate(sample._1, y._1)).isDefined)
        return false
    })
    return true
  }
}
