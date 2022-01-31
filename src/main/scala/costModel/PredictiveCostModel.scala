package costModel

import java.io.File
import java.util

import definition.Paths
import definition.Paths.{ParquetNameToSynopses, SynopsesToParquetName, extractAccessedColumn, getGroupByKeys, getTables, pathToSaveSynopses, updateAttributeName, warehouseParquetNameToSize, windowSize, _}
import operators.physical.{SampleExec, UniformSampleExec2, UniversalSampleExec2}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReturnAnswer}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}

import scala.collection.immutable.ListSet
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.collection.{Seq, mutable}
import scala.io.Source
import scala.reflect.io.Directory

class PredictiveCostModel(sparkSession: SparkSession, isLRU: Boolean, justAPP: Boolean) extends CostModelAbs {
  val future = new mutable.Queue[Seq[Seq[SparkPlan]]]() // ( q1( sub1(app1,app2) , sub2(app1,app2) ),  q2( sub1(app1,app2) , sub2(app1,app2) ) )
  val past = new mutable.HashMap[String, mutable.Queue[(Long, Seq[Seq[SparkPlan]], LogicalPlan, String)]]() // ( ip1:( q1:( sub1:(app1,app2) , sub2:(app1,app2) ),  q2:( sub1:(app1,app2) , sub2:(app1,app2) ) ) )
  var currentExact: Seq[Seq[SparkPlan]] = null
  var currentSubQueriesAPP: Seq[Seq[SparkPlan]] = null
  var currentQuery: LogicalPlan = null
  var currentQueryVector = ""
  var epochCurrentQuery: Long = 0
  var currentIp = ""
  val indexToAccessedCol: mutable.HashMap[Int, String] = new mutable.HashMap[Int, String]()
  val indexToGroupByKey: mutable.HashMap[Int, String] = new mutable.HashMap[Int, String]()
  val indexToJoinKey: mutable.HashMap[Int, String] = new mutable.HashMap[Int, String]()
  val indexToTable: mutable.HashMap[Int, String] = new mutable.HashMap[Int, String]()
  val accessedColToVectorIndex: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
  val groupByKeyToVectorIndex: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
  val joinKeyToVectorIndex: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
  val tableToVectorIndex: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
  var vectorSize = 0
  val timeBucket = 14
  val url = "http://localhost:5000/vec?vec="
  readVectorInfo()
  val accessColIndexRange = (0, indexToAccessedCol.size - 1)
  val groupByIndexRange = (indexToAccessedCol.size, indexToAccessedCol.size + indexToGroupByKey.size - 1)
  val joinKeyIndexRange = (indexToAccessedCol.size + indexToGroupByKey.size, indexToAccessedCol.size + indexToGroupByKey.size + indexToJoinKey.size - 1)
  val tableIndexRange = (indexToAccessedCol.size + indexToGroupByKey.size + indexToJoinKey.size, indexToAccessedCol.size + indexToGroupByKey.size + indexToJoinKey.size + indexToTable.size - 1)
  val setSelectionStrategy = if (isLRU) new LRU(this) else new CELF(this)
  val futureProjectList = new ListBuffer[String]()
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  override def getFutureProjectList(): Seq[String] = futureProjectList

  def getPrediction(vec: String): String = scala.io.Source.fromURL(url + vec).mkString

  def getFuture = (past.map(_._1) ++ Seq(currentIp)).toSet.flatMap(getFutureForAtiveSession).toList.sortBy(_._2).map(_._1)

  def getPredictedQuerySubPlanApp(vecs: String) = {
    X(vecs.grouped(featureCNT ).toList.map(vectorToLogicalPlan2).filter(_ != null)).flatten(x => x._1.map(y => (y, x._2))).map(Query => ( {
      updateAttributeName(Query._1, new mutable.HashMap[String, Int]())
      val joins = enumerateRawPlanWithJoin(Query._1)
      val logicalPlans = joins.map(x => sparkSession.sessionState.optimizer.execute(x))
      Seq(logicalPlans.flatMap(x => sparkSession.sessionState.planner.plan(ReturnAnswer(x))))
    }, Query._2))
  }

  def X(s: Seq[(Seq[LogicalPlan], Int)]): Seq[(Seq[LogicalPlan], Int)] = {
    if (s.size == 0)
      return s
    var y = s.map(_._2).toList
    var t = new Array[Int](s.size)
    t(0) = y(0)
    for (i <- (1 to y.size - 1)) {
      t(i) = y(i) + t(i - 1)
    }
    return s.map(_._1).zip(t)

  }


  def getFutureForAtiveSession(ip: String): Seq[(Seq[Seq[SparkPlan]], Int)] = {
    //  if (ip.equals(currentIp)) {
    //    val pastQuery = past.get(ip)
    //    if (pastQuery.isDefined)
    //      getPredictedQuerySubPlanApp(getPrediction(pastQuery.get.map(_._4).mkString(delimiterVector) + delimiterVector + currentQueryVector))
    //    else
    //       getPredictedQuerySubPlanApp(getPrediction(currentQueryVector))
    //  }
    //   else {
    val pastQuery = past.get(ip)
    if (pastQuery.isDefined)
      getPredictedQuerySubPlanApp(getPrediction(pastQuery.get.map(_._4).mkString(delimiterVector)))
    else Seq()
    //   }
  }

  /*
    def calRewardOf(A: Seq[(String, Long)]): Double = {
      //l prediction.foreach(println)
      getCheapestExactCostOf(prediction ++ Seq(currentSubQueriesAPP)) - (prediction ++ Seq(currentSubQueriesAPP)).map(perSubqueryPhysicalPlan => perSubqueryPhysicalPlan.map(subQueryAPPs => subQueryAPPs.map(APP => costOfAppPlan(APP, A)._2).min).reduce(_ + _)).reduce(_ + _)
    }



    def calRewardOf22222(app: SparkPlan): Double = {
      val synopsesOfApp = extractSynopses(app).map(y => (y.toString(), costOfAppPlan(y, getWRSynopsesSize.toSeq)._1))
      getCheapestExactCostOf(prediction, app) - (costOfAppPlan(app, getWRSynopsesSize.toSeq)._2 + (if (prediction.size == 0) 0 else prediction.map(query => query.map(subQuery => subQuery.map(APP => costOfAppPlan(APP, synopsesOfApp)._2).min).reduce(_ + _)).reduce(_ + _)))
    }
  */
  var prediction: Seq[Seq[Seq[SparkPlan]]] = null

  def calRewardOf3(app: SparkPlan): Double = {

    val synopsesOfApp = extractSynopses(app).map(y => (y.toString(), synopsesSize.getOrElse(y.toString(), costOfAppPlan(y, getWRSynopsesSize.toSeq)._1)))
    if (synopsesOfApp.size == 0)
      return costOfAppPlan(app, getWRSynopsesSize.toSeq)._2 + (if (prediction.size == 0) 0 else prediction.map(query => query.map(subQuery => subQuery.map(APP => costOfExact(APP)._2).min).reduce(_ + _)).reduce(_ + _))
    if (synopsesOfApp.reduce((a, b) => ("", a._2 + b._2))._2 > maxSpace) {
      // synopsesOfApp.foreach(println)
      //   println("-----")
      return Double.MaxValue
    }
    (costOfAppPlan(app, getWRSynopsesSize.toSeq)._2 + (if (prediction.size == 0) 0 else prediction.map(query => query.map(subQuery => subQuery.map(APP => costOfAppWithFixedSynopses(APP, synopsesOfApp)._2).min).reduce(_ + _)).reduce(_ + _)))
  }


  // cheapestExact(P+Can_app)- { Exec(Can_app,WR) + Exec_min( P , S(Can_app) ) }

  override def suggest(): Seq[SparkPlan] = {
    //println("--------------------------------------------------------------------------")
    //currentSubQueriesAPP.map(subQueryAPPs => subQueryAPPs.foreach(pp => println(toStringTree(pp))))
    //println("--------------------------------------------------------------------------")
    // prediction.foreach(println)
    val warehouse = warehouseParquetNameToSize.map(x => ParquetNameToSynopses(x._1)).toSeq
    val temp = currentSubQueriesAPP.map(apps => apps.find(app => AreCovered(extractSynopses(app).map(_.asInstanceOf[SampleExec]), warehouse)))
    if(temp.map(_.isDefined).reduce(_&&_))
      return temp.map(_.get)
    (currentSubQueriesAPP).map(subQueryAPPs => {
      //println("---")
      subQueryAPPs.map(pp => {
        //     println(toStringTree(pp))
        //     println(calRewardOf3(pp))
        (pp, calRewardOf3(pp)) /*calRewardOf(extractSynopses(pp).map(y => (y.toString(), costOfAppPlan(y, Seq())._1))))*/
      }).minBy(_._2)._1
    })
  }

  /*def calRewardOfGain(app: SparkPlan, windowSize: Int): Double = {
    val synopsesOfApp = extractSynopses(app).map(y => (y.toString(), costOfAppPlan(y, getWRSynopsesSize.toSeq)._1))
    if (synopsesOfApp.size == 0)
      return 1.0
    if(synopsesOfApp.reduce((a,b)=>("",a._2+b._2))._2>maxSpace)
      return -1
    if(prediction.size==0)
      return 0
    prediction.map(subQueries => subQueries.map(pps => {
      pps.map(costOfExact).minBy(_._2)._2 - pps.map(pp => costOfAppWithFixedSynopses(pp, synopsesOfApp)).minBy(_._2)._2
    }).reduce(_ + _)).reduce(_ + _)
  }

  // cheapestExact(F+Can_app)- { Exec(Can_app,WR) + Exec_min( F , S(Can_app) ) }

  override def suggest(): Seq[SparkPlan] = {
    //println("--------------------------------------------------------------------------")
    //currentSubQueriesAPP.map(subQueryAPPs => subQueryAPPs.foreach(pp => println(toStringTree(pp))))
    //println("--------------------------------------------------------------------------")

    currentSubQueriesAPP.map(subQueryAPPs => {
      subQueryAPPs.map(pp => {
        //  println(toStringTree(pp))
        //   println(calRewardOfGain(pp, windowSize))
        (pp, calRewardOfGain(pp, windowSize)) /*calRewardOf(extractSynopses(pp).map(y => (y.toString(), costOfAppPlan(y, Seq())._1))))*/
      }).maxBy(_._2)._1
    })
  }
  */
  /*override def suggest(): Seq[SparkPlan] = {
    currentSubQueriesAPP.map(subQueryAPPs => {
      subQueryAPPs.map(pp => {
        (pp, calRewardOf(extractSynopses(pp).map(y => (y.toString(), costOfAppPlan(y, Seq())._1))))
      }).maxBy(_._2)._1
    })
  }*/

  override def addQuery(query: String, ip: String, epoch: Long, f: Seq[String]): Unit = {
    epochCurrentQuery = epoch
    currentIp = ip
    currentQuery = sparkSession.sqlContext.sql(query).queryExecution.analyzed
    var tt: Long = 0

    val x = past.get(ip)
    if (!x.isDefined)
      past.put(ip, new mutable.Queue[(Long, Seq[Seq[SparkPlan]], LogicalPlan, String)]())
    else if (x.get.last._1 + gap < epochCurrentQuery)
      past.get(ip).get.clear()
    if (past.get(ip).get.size > 0)
      tt = epoch - past.get(ip).get.last._1
    past.get(ip).get.enqueue((epoch, currentSubQueriesAPP, currentQuery, queryToFeatureVector(currentQuery, tt.toInt)))

    removeOldProcesses()
    val ipQueryCount = past.getOrElse(ip, Seq()).size
    //  println(past.map(_._1))
    if (ipQueryCount > windowSize)
      past.get(ip).get.dequeue()
    prediction = getFuture.filter(x => x(0).size > 0) //++ p //++past.map(x=>x._2.head)

    //  past.foreach(println)
    val j = getJoinKeys()

    // currentQueryVector = queryToFeatureVector(currentQuery)
    currentSubQueriesAPP = getAggSubQueries(currentQuery).map(subQuery => {
      updateAttributeName(subQuery, new mutable.HashMap[String, Int]())
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
      }))
      val temp = sparkSession.experimental.extraOptimizations
      sparkSession.experimental.extraOptimizations = Seq()
      val exact = sparkSession.sessionState.planner.plan(ReturnAnswer(sparkSession.sessionState.optimizer.execute(subQuery)))
      sparkSession.experimental.extraOptimizations = temp
      if (justAPP)
        apps
      else
        apps ++ exact
    })
  /*  currentSubQueriesAPP = getAggSubQueries(currentQuery).map(subQuery => {
      updateAttributeName(subQuery, new mutable.HashMap[String, Int]())
      val joins = enumerateRawPlanWithJoin(subQuery)
      val logicalPlans = joins.map(x => sparkSession.sessionState.optimizer.execute(x))
      val apps = logicalPlans.flatMap(x => sparkSession.sessionState.planner.plan(ReturnAnswer(x)))
      val temp = sparkSession.experimental.extraOptimizations
      sparkSession.experimental.extraOptimizations = Seq()
      val exact = sparkSession.sessionState.planner.plan(ReturnAnswer(sparkSession.sessionState.optimizer.execute(subQuery)))
      sparkSession.experimental.extraOptimizations = temp
      if (justAPP)
        apps
      else
        apps ++ exact
    })*/
    // futureProjectList.clear()
    //prediction.foreach(x=>x.foreach(println))
  }

  def getJoinKeys(): Seq[AttributeReference] = prediction.flatMap(
    subQueries => subQueries.flatMap(subQueryPPs => subQueryPPs(0).flatMap(node => {
      if (node.isInstanceOf[ShuffledHashJoinExec])
        Seq(definition.Paths.getAttRefFromExp(node.asInstanceOf[ShuffledHashJoinExec].leftKeys(0))(0), definition.Paths.getAttRefFromExp(node.asInstanceOf[ShuffledHashJoinExec].rightKeys(0))(0))
      else if (node.isInstanceOf[BroadcastHashJoinExec])
        Seq(definition.Paths.getAttRefFromExp(node.asInstanceOf[BroadcastHashJoinExec].leftKeys(0))(0), definition.Paths.getAttRefFromExp(node.asInstanceOf[BroadcastHashJoinExec].rightKeys(0))(0))
      else if (node.isInstanceOf[SortMergeJoinExec])
        Seq(definition.Paths.getAttRefFromExp(node.asInstanceOf[SortMergeJoinExec].leftKeys(0))(0), definition.Paths.getAttRefFromExp(node.asInstanceOf[SortMergeJoinExec].rightKeys(0))(0))
      else Seq()
    }))).distinct


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


  def vectorToLogicalPlan2(vec: String): (Seq[LogicalPlan], Int) = {
    var accessCols = new ListBuffer[String]()
    // var accessCols2 = ""
    var groupBy = new ListBuffer[String]()
    var joinKey = new ListBuffer[String]()
    val queries = new ListBuffer[String]()
    var tables = new ListBuffer[String]()
    var arrivalRate = 0
    var flag = true
    val bits = vec.split("")
    var query = "Select "
    for (index <- (0 to vectorSize - 1)) if (bits(index) == "1") {
      if (accessColIndexRange._1 <= index && index <= accessColIndexRange._2) {
        val ac = indexToAccessedCol.get(index).get
        accessCols.+=(ac)
        //  accessCols2 += indexToAccessedCol.get(index).get.split("\\.")(0) + ","
        // val x = ac.substring(1 + ac.indexOf('('), ac.indexOf(')')).split("\\.")
        val x = ac.split("\\.")
        futureProjectList.+=(x(0) + "_1." + x(1))
      }
      else if (groupByIndexRange._1 <= index && index <= groupByIndexRange._2) {
        groupBy.+=(indexToGroupByKey.get(index).get)
      }
      else if (joinKeyIndexRange._1 <= index && index <= joinKeyIndexRange._2) {
        joinKey.+=(indexToJoinKey.get(index).get)
      }
      else if (tableIndexRange._1 <= index && index <= tableIndexRange._2) {
        tables.+=(indexToTable.get(index).get)
      }
      else if (tableIndexRange._2 < index && index <= tableIndexRange._2 + timeBucket) {
        val iii = index - tableIndexRange._2
        /*iii match {
          case 1 => arrivalRate = 90
          case 2 => arrivalRate = 270
          case 3 => arrivalRate = 450
          case 4 => arrivalRate = 630
          case 5 => arrivalRate = 810
          case 6 => arrivalRate = 990
          case 7 => arrivalRate = 1170
          case 8 => arrivalRate = 1350
          case 9 => arrivalRate = 1530
        }*/
        if (flag)
          (iii) match {
            case 1 => arrivalRate = 0
            case 2 => arrivalRate = 1
            case 3 => arrivalRate = 2
            case 4 => arrivalRate = 4
            case 5 => arrivalRate = 7
            case 6 => arrivalRate = 10
            case 7 => arrivalRate = 13
            case 8 => arrivalRate = 21
            case 9 => arrivalRate = 40
            case 10 => arrivalRate = 78
            case 11 => arrivalRate = 150
            case 12 => arrivalRate = 300
            case 13 => arrivalRate = 600
            case 14 => arrivalRate = 1100
            case _ => throw new Exception("invalid vector index")
          }
        flag = false
      }
      else throw new Exception("Index is not in range of vector information")
    }
    //accessCols.filter(f => f.split("\\.")(0).equals(x) && !groupBy.contains(f)).mkString(",")
    if (accessCols.size == 0 && tables.size == 0)
      return null
    //   println(".......")
    //   println(accessCols)
    //   println(groupBy)
    //   println(joinKey)

    if (joinKey.size == 0) {
      if (groupBy.size > 0) {
        val t = groupBy.map(_.split("\\.")(0)).toSet
        for (x <- t)
          queries.+=("select count(*) from " + x + " group by " + groupBy.filter(v => v.split("\\.")(0).equals(x)).mkString(","))
      }
      else {
        if (tables.size > 0)
          queries += ("select count(*) from " + tables(0))
        // else if (accessCols.size == 0)
        //   queries += ("select count(*) from " + tables(0))
        // else
        //  queries.+=("select count(*) from " + accessCols.groupBy(x => x.split("\\.")(0)).map(x => (x._1, x._2.size)).maxBy(_._2)._1)
      }
    }
    else if (joinKey.size == 1) {
      val tt = joinKey(0).split("=").map(_.split("\\.")(0)).distinct
      if (groupBy.filter(v => tt.contains(v.split("\\.")(0))).size == 0)
        queries.+=("select count(*) from " + tt.mkString(",") + " where " + joinKey(0) + " ")
      else
        queries.+=("select count(*) from " + tt.mkString(",") + " where " + joinKey(0) + " group by " + groupBy.filter(v => tt.contains(v.split("\\.")(0))).mkString(","))
    }
    else if (joinKey.size == 2) {
      val t = joinKey(0).split("=").map(_.split("\\.")(0)).distinct
      var tt = joinKey(1).split("=").map(_.split("\\.")(0)).distinct
      if (t.intersect(tt).size == 0) {
        if (groupBy.filter(v => t.contains(v.split("\\.")(0))).size == 0)
          queries.+=("select count(*) from " + t.mkString(",") + " where " + joinKey(0) + " ")
        else
          queries.+=("select count(*) from " + t.mkString(",") + " where " + joinKey(0) + " group by " + groupBy.filter(v => t.contains(v.split("\\.")(0))).mkString(","))
        if (groupBy.filter(v => tt.contains(v.split("\\.")(0))).size == 0)
          queries.+=("select count(*) from " + tt.mkString(",") + " where " + joinKey(1) + " ")
        else
          queries.+=("select count(*) from " + tt.mkString(",") + " where " + joinKey(1) + " group by " + groupBy.filter(v => tt.contains(v.split("\\.")(0))).mkString(","))
      }
      else {
        tt = (t ++ tt).distinct
        if (groupBy.filter(v => tt.contains(v.split("\\.")(0))).size == 0)
          queries.+=("select count(*) from " + tt.mkString(",") + " where " + joinKey.mkString(" and ") + " ")
        else
          queries.+=("select count(*) from " + tt.mkString(",") + " where " + joinKey.mkString(" and ") + " group by " + groupBy.filter(v => tt.contains(v.split("\\.")(0))).mkString(","))
      }
    }
    else {
      //for (jk <- joinKey) {
      val tables = joinKey.flatMap(_.split("=")).map(_.split("\\.")(0)).toSet
      // val tt = jk.split("=").map(_.split("\\.")(0)).distinct
      if (groupBy.filter(v => tables.contains(v.split("\\.")(0))).size == 0)
        queries.+=("select count(*) from " + tables.mkString(",") + " where " + joinKey.mkString(" and ") + " ")
      else
        queries.+=("select count(*) from " + tables.mkString(",") + " where " + joinKey.mkString(" and ") + " group by " + groupBy.filter(v => tables.contains(v.split("\\.")(0))).mkString(","))
      // }
    }
    //queries.foreach(println)
    //println(arrivalRate)
    //println(".......")

    if (queries.size == 0)
      return null
    (queries.map(x => (sparkSession.sql(x).queryExecution.analyzed)), arrivalRate)
  }

  def vectorToLogicalPlan(vec: String): LogicalPlan = {
    var accessCols = ""
    var accessCols2 = ""
    var groupBy = ""
    var joinKey = ""
    var tables = ""
    val bits = vec.split("")
    var query = "Select "
    for (index <- (0 to vectorSize - 1)) if (bits(index) == "1") {
      if (accessColIndexRange._1 <= index && index <= accessColIndexRange._2) {
        accessCols += "count(" + indexToAccessedCol.get(index).get + "),"
        accessCols2 += indexToAccessedCol.get(index).get.split("\\.")(0) + ","
        futureProjectList.+=(indexToAccessedCol.get(index).get)
      }
      else if (groupByIndexRange._1 <= index && index <= groupByIndexRange._2) {
        groupBy += indexToGroupByKey.get(index).get + ","
      }
      else if (joinKeyIndexRange._1 <= index && index <= joinKeyIndexRange._2) {
        joinKey += indexToJoinKey.get(index).get + " and "
      }
      else if (tableIndexRange._1 <= index && index <= tableIndexRange._2) {
        tables += indexToTable.get(index).get + ","
      }
      else throw new Exception("Index is not in range of vector information")
    }
    var t = new ListSet[String]()
    if (accessCols2.size > 0)
      t.++=(accessCols2.dropRight(1).split(","))
    //  if (tables.size > 0)
    //    t.++=(tables.dropRight(1).split(","))
    if (joinKey.size > 0)
      t.++=(joinKey.dropRight(5).split(" and ").flatMap(x => x.split("=").map(_.split("\\.")(0))))
    if (groupBy.size > 0)
      t.++=(groupBy.dropRight(1).split(",").map(_.split("\\.")(0)))

    query = ("select " + (if (groupBy.size > 0) groupBy else "") + " count(*)"
      // + (if (accessCols.size > 0) (accessCols.dropRight(1)) else if (groupBy.size > 0) groupBy.dropRight(1) else " count(*) ")
      + " from " + t.mkString(",")
      /*else if (joinKey.size > 0 && groupBy.size > 0)
        (joinKey.dropRight(5).split(" and ").flatMap(x => x.split("=").map(_.split("\\.")(0))) ++ groupBy.dropRight(1).split(",").map(_.split("\\.")(0))).mkString(",") + " "
      else if (joinKey.size > 0) (joinKey.dropRight(5).split(" and ").flatMap(x => x.split("=").map(_.split("\\.")(0)))).mkString(",") + " "
      else (groupBy.dropRight(1).split(",").map(_.split("\\.")(0))).mkString(",") + " ")*/
      + (if (joinKey.size > 0) (" where " + joinKey.dropRight(5)) else " ")
      + (if (groupBy.size > 0) (" group by " + groupBy.dropRight(1)) else " "))
    // println(query)
    // println(sparkSession.sql(query).queryExecution.analyzed)
    //   println("----------------------------------------------------")
    //  enumerateRawPlanWithJoin(sparkSession.sql(query).queryExecution.analyzed).foreach(println)
    //   println("====================================================")

    if (t.size == 0)
      return null
    sparkSession.sql(query).queryExecution.analyzed
  }


  def readVectorInfo(): Unit = {
    var lines = Source.fromFile(pathToML_Info + tag + "_colIndex").getLines
    while (lines.hasNext) {
      val entry = (lines.next()).split(delimiterHashMap)
      indexToAccessedCol.put(entry(1).toInt, entry(0))
      accessedColToVectorIndex.put(entry(0), entry(1).toInt)
    }
    lines = Source.fromFile(pathToML_Info + tag + "_groupIndex").getLines
    while (lines.hasNext) {
      val entry = (lines.next()).split(delimiterHashMap)
      indexToGroupByKey.put(entry(1).toInt, entry(0))
      groupByKeyToVectorIndex.put(entry(0), entry(1).toInt)
    }
    lines = Source.fromFile(pathToML_Info + tag + "_joinIndex").getLines
    while (lines.hasNext) {
      val entry = (lines.next()).split(delimiterHashMap)
      indexToJoinKey.put(entry(1).toInt, entry(0))
      joinKeyToVectorIndex.put(entry(0), entry(1).toInt)
    }
    lines = Source.fromFile(pathToML_Info + tag + "_tableIndex").getLines
    while (lines.hasNext) {
      val entry = (lines.next()).split(delimiterHashMap)
      vectorSize = entry(1).toInt
      indexToTable.put(entry(1).toInt, entry(0))
      tableToVectorIndex.put(entry(0), entry(1).toInt)
    }
    vectorSize += (1 + timeBucket)
  }


  override def getFutureAPP(): Seq[Seq[Seq[SparkPlan]]] = prediction

  override def getFutureSize(): Long = prediction.size


  def removeOldProcesses(): Unit =
    for (ip <- past)
      if (ip._2.last._1 + gap < epochCurrentQuery)
        past.remove(ip._1)


  def queryToFeatureVector(lpp: LogicalPlan, gap: Int): String = {
    val tableName: HashMap[String, String] = new HashMap()
    tableName.clear()
    updateAttributeName2(lpp, tableName)
    getAggSubQueries(lpp).map(lp => {
      val aggregateCol = Paths.getAggregateColumns(lp)
      val joinKeys = Paths.getJoinConditions(lp).distinct.sortBy(_.toString).filter(!_.contains("userDefinedColumn"))
      val groupByKeys = getGroupByKeys(lp).distinct.sortBy(_.toString).filter(!_.contains("userDefinedColumn"))
      val tables = getTables(lp).distinct.sortBy(_.toString)
      val vector = new Array[Int](vectorSize + reserveFeature)
      util.Arrays.fill(vector, 0)
      for (accCol <- aggregateCol) if (accessedColToVectorIndex.get(accCol).isDefined)
        vector(accessedColToVectorIndex.get(accCol).get) = 1
      for (groupCol <- groupByKeys) if (groupByKeyToVectorIndex.get(groupCol).isDefined)
        vector(groupByKeyToVectorIndex.get(groupCol).get) = 1
      for (joinCol <- joinKeys) if (joinKeyToVectorIndex.get(joinCol).isDefined)
        vector(joinKeyToVectorIndex.get(joinCol).get) = 1
      for (table <- tables) if (tableToVectorIndex.get(table).isDefined)
        vector(tableToVectorIndex.get(table).get) = 1
      if (timeBucket > 0)
        vector(tableIndexRange._2 + gabToIndex(gap) + 1) = 1
      vector.mkString("")
    }).mkString(";")
  }


  def processToFeatureVector(ip: String): Seq[String] = {
    val process = past.get(ip).get.sortBy(_._1)
    val tableName: HashMap[String, String] = new HashMap()
    val res = new ListBuffer[String]()
    for (query <- process) {
      val lp = query._3
      tableName.clear()
      updateAttributeName2(lp, tableName)
      val accessedColsSet = new mutable.HashSet[String]()
      extractAccessedColumn(lp, accessedColsSet)
      val accessedCols = accessedColsSet.toSeq.distinct.sortBy(_.toString).filter(!_.contains("userDefinedColumn"))
      val joinKeys = Paths.getJoinConditions(lp).distinct.sortBy(_.toString).filter(!_.contains("userDefinedColumn"))
      val groupByKeys = getGroupByKeys(lp).distinct.sortBy(_.toString).filter(!_.contains("userDefinedColumn"))
      val tables = getTables(lp).distinct.sortBy(_.toString)
      val vector = new Array[Int](vectorSize + reserveFeature)
      util.Arrays.fill(vector, 0)
      for (accCol <- accessedCols) if (accessedColToVectorIndex.get(accCol).isDefined)
        vector(accessedColToVectorIndex.get(accCol).get) = 1
      for (groupCol <- groupByKeys) if (groupByKeyToVectorIndex.get(groupCol).isDefined)
        vector(groupByKeyToVectorIndex.get(groupCol).get) = 1
      for (joinCol <- joinKeys) if (joinKeyToVectorIndex.get(joinCol).isDefined)
        vector(joinKeyToVectorIndex.get(joinCol).get) = 1
      for (table <- tables) if (tableToVectorIndex.get(table).isDefined)
        vector(tableToVectorIndex.get(table).get) = 1
      vector.mkString("")
      res += (vector.mkString(""))
    }
    res.toSeq
  }
}
