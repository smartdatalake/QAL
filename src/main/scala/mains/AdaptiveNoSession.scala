package mains

import java.nio.file.{Files, Paths}
import java.util
import java.util.Date

import costModel.PredictiveCostModel
import definition.ModelInfo
import definition.Paths._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import rules.logical.{ApproximateInjector, pushFilterUp}
import rules.physical.SampleTransformationMultiple

import scala.collection.mutable.{HashMap, ListBuffer}
import scala.collection.{Seq, mutable}
import scala.io.Source


object AdaptiveNoSession extends QueryEngine_Abs("MLLSTMNoSession") {
  val sessionPool: ListBuffer[String] = ListBuffer()
  val sessions = new mutable.Queue[(Long, LogicalPlan, String)]() // ( ip1:( q1:( sub1:(app1,app2) , sub2:(app1,app2) ),  q2:( sub1:(app1,app2) , sub2:(app1,app2) ) ) )
  //val indexToAccessedCol: mutable.HashMap[Int, String] = new mutable.HashMap[Int, String]()
  //val indexToGroupByKey: mutable.HashMap[Int, String] = new mutable.HashMap[Int, String]()
  //val indexToJoinKey: mutable.HashMap[Int, String] = new mutable.HashMap[Int, String]()
  //val indexToTable: mutable.HashMap[Int, String] = new mutable.HashMap[Int, String]()
  //val accessedColToVectorIndex: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
  //val groupByKeyToVectorIndex: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
  //val joinKeyToVectorIndex: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
  //val tableToVectorIndex: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
  //var vectorSize = 0
  pathToML_Info = "/home/hamid/QAL/DensityCluster/"
  val tag = "_train_gap18000000_processMinLength5_maxQueryRepetition10000000_featureMinFRQ1_reserveFeature15"
  val tag2 = "_test_gap18000000_processMinLength5_maxQueryRepetition10000000_featureMinFRQ1_reserveFeature15"
  val timeBucket = 14
  val url = "http://localhost:5000/vec?vec="
  val url2 = "http://localhost:5000/test?vec="
  var is6month = true
  //var accessColIndexRange = (0, 0)
  //var groupByIndexRange = (0, 0)
  //var joinKeyIndexRange = (0, 0)
  //var tableIndexRange = (0, 0)
  val setup: Seq[String] = Seq("4YearTo6Month", "2WeekTo2Week")
  //, "1DayTo1Day")
  val modelInfo: mutable.HashMap[(String, Int), ModelInfo] = new mutable.HashMap[(String, Int), ModelInfo]()
  ReadModelInfo()
  val m = modelInfo.toSeq.sortBy(_._1._2).reverse
  var modelTag: String = ""
  var modelNumber: Int = 0
  var currentModel: ModelInfo = null
  val k = 10
  var PreVal4Yto2W = 0
  var PreVal4Yto6M = 0
  var PreVal2Wto2W = 0
  var PreVal1Dto1D = 0
  var currentEpoch: Long = 0
  var m_2Wto2W: ((String, Int), ModelInfo) = null
  //var m_1Dto1D: ((String, Int), ModelInfo) = null
  var m_4Yto2W: ((String, Int), ModelInfo) = null
  var m_4Yto6M: ((String, Int), ModelInfo) = null
  val begin = new Date(2008, 0, 1, 0, 0, 0).getTime / 1000
  var previousDay = 0

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  def main(args: Array[String]): Unit = {
    readConfiguration(args)
    loadTables(sparkSession)
    mapRDDScanRowCNT = readRDDScanRowCNT(sparkSession)
    val costModel = new PredictiveCostModel(sparkSession, isLRU, justAPP, isAdaptive)
    //  if (isExtended)
    sparkSession.experimental.extraStrategies = Seq(SampleTransformationMultiple)
    // else sparkSession.experimental.extraStrategies = Seq(SampleTransformation)
    sparkSession.experimental.extraOptimizations = Seq(new ApproximateInjector(confidence, error, seed), new pushFilterUp)


    timeTotalRuntime = System.nanoTime()
    queries = loadWorkloadWithIP("skyServer", sparkSession)
    currentEpoch = queries(0)._3
   // m_4Yto2W = m.find(x => x._1._1.equalsIgnoreCase("4YearTo2Week") && x._1._2 <= ((currentEpoch - begin) / (2 * WEEK)).toInt).get
    m_4Yto6M = m.find(x => x._1._1.equalsIgnoreCase("4YearTo6Month") && x._1._2 <= ((currentEpoch - begin) / (6 * MONTH)).toInt).get
    m_2Wto2W = m.find(x => x._1._1.equalsIgnoreCase("2WeekTo2Week") && x._1._2 <= ((currentEpoch - begin) / (2 * WEEK)).toInt).get
    //m_1Dto1D = m.find(x => x._1._1.equalsIgnoreCase("1DayTo1Day") && x._1._2 <= ((currentEpoch - begin) / (1 * DAY)).toInt).get
    currentModel = m_4Yto6M._2
    modelTag = m_4Yto6M._1._1
    modelNumber = m_4Yto6M._1._2
    var timeCHK: Long = 0
    var timeExec: Long = 0
    for (query <- queries) if (queryCNT <= testSize) {
      currentEpoch = query._3
      sparkSession.sqlContext.clearCache()
      outputOfQuery = ""
      println(i + "qqqqqqqqqqqqqqqqqq" + query)
      i += 1

      timeCHK = System.nanoTime()
      //  if(new Date(currentEpoch*1000).getDate!=previousDay) {
      //     previousDay=new Date(currentEpoch*1000).getDate
      //    }
      //     else println("xxxxxxxxx")

    //  if ((i % step == 0 || i % step == stepper)) {
    //    selectModel()
   //   }
     // if (isAdaptive && i > 0 && (i % step == 0 || i % step == stepper)) {
     //   windowSize = costModel.UpdateWindowHorizon()
    //    println(windowSize)
   //   }
      timeUpdateWindowHorizon += System.nanoTime() - timeCHK


      timeCHK = System.nanoTime()
      val future = ReadNextQueries(query._1, query._2, query._3, i)
      timeReadNextQuery += System.nanoTime() - timeCHK

      timeCHK = System.nanoTime()
      costModel.addQuery(query._1, future, futureProjectList.distinct)
      timeAPPGeneration += System.nanoTime() - timeCHK

      timeCHK = System.nanoTime()
      val appPhysicalPlan = costModel.suggest()
      timePlanSuggestion += System.nanoTime() - timeCHK

      timeCHK = System.nanoTime()
      for (subqueryAPP <- appPhysicalPlan) {
        var cheapest = changeSynopsesWithScan(prepareForExecution(subqueryAPP, sparkSession))
        // println(cheapest)
        if (isExtended)
          cheapest = ExtendProject(cheapest, costModel.getFutureProjectList().distinct)
        executeAndStoreSample(cheapest, sparkSession)
        cheapest = changeSynopsesWithScan(cheapest)
        countReusedSample(cheapest)
        //println(cheapest)
        cheapest.executeCollectPublic().toList.sortBy(_.toString()).foreach(row => {
          outputOfQuery += row.toString() + "\n"
          counterNumberOfGeneratedRow += 1
        })
      }
      timeExec += (System.nanoTime() - timeCHK)

      println((System.nanoTime() - timeCHK) / 1000000000)
      println(timeExec / 1000000000)

      timeCHK = System.nanoTime()
      costModel.updateWarehouse()
      timeForUpdateWarehouse += System.nanoTime() - timeCHK

      //   results += ((query._1, outputOfQuery, checkpointForAppQueryExecution, checkpointForAppQueryPlanning
      //      , checkPointForSampling, checkAct, checkWare, checkPointForExecution))
      //     val agg = results.reduce((a, b) => ("", "", a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7, a._8 + b._8))
      //     println(checkPointForExecution / 1000000000 + "," + agg._8 / 1000000000)

      queryCNT += 1
    }
    timeTotalRuntime = System.nanoTime() - timeTotalRuntime
    printReport()





    //execute(costModel)


    flush()
  }


  override def ReadNextQueries(query: String, ip: String, epoch: Long, queryIndex: Int): Seq[String] = {
    val currentQuery = sparkSession.sqlContext.sql(query).queryExecution.analyzed
    var tt: Long = 0

    sessions.enqueue((epoch, currentQuery, queryToFeature(currentQuery, tt.toInt)))

    //  println(past.map(_._1))
    if (sessions.size > 10)
      sessions.dequeue()
   getNextQueriesForActiveSession() //.filter(x => x(0).size > 0) //++ p //++past.map(x=>x._2.head)
  }

  override def readConfiguration(args: Array[String]): Unit = {
    super.readConfiguration(args)
    pathToML_Info = "/home/hamid/QAL/DensityCluster/"
    if (args.size > 20)
      is6month = args(20).toBoolean
  }

  def getNextQueriesForActiveSession(): Seq[String] = {
    val pastQuery = sessions.toSeq
    if (pastQuery.size>0) {
      System.in.close()
      getQueriesText(scala.io.Source.fromURL(url + modelTag + "X" + modelNumber + "X" + pastQuery.flatMap(_._3.split(";")).map(featureToVector).mkString(delimiterVector) + "X" + windowSize).mkString)
      //getQueriesText(scala.io.Source.fromURL(url + pastQuery.get.map(_._3).mkString(delimiterVector) + "X" + windowSize).mkString)
    } else Seq()
  }

  def getAccuracy(sessions: Seq[String], modelTag: String, modelNumber: Int, model: ModelInfo) = {
    scala.io.Source.fromURL(url2 + modelTag + "X" + modelNumber + "X" + sessions.map(session => session.split(";").map(x => featureToVector(x, model)).mkString(delimiterVector)).mkString("P") + "X" + windowSize).mkString
  }

  def featureToVector(features: String): String = {
    val vector = new Array[Int](currentModel.vectorSize)
    util.Arrays.fill(vector, 0)
    val aggregateCol = features.split("@")(0).split(",")
    val groupByKeys = features.split("@")(2).split(",")
    val joinKeys = features.split("@")(1).split(",")
    val tables = features.split("@")(3).split(",")
    val gap = features.split("@")(4).toInt
    for (accCol <- aggregateCol) if (currentModel.accessedColToVectorIndex.get(accCol).isDefined)
      vector(currentModel.accessedColToVectorIndex.get(accCol).get) = 1
    for (groupCol <- groupByKeys) if (currentModel.groupByKeyToVectorIndex.get(groupCol).isDefined)
      vector(currentModel.groupByKeyToVectorIndex.get(groupCol).get) = 1
    for (joinCol <- joinKeys) if (currentModel.joinKeyToVectorIndex.get(joinCol).isDefined)
      vector(currentModel.joinKeyToVectorIndex.get(joinCol).get) = 1
    for (table <- tables) if (currentModel.tableToVectorIndex.get(table).isDefined)
      vector(currentModel.tableToVectorIndex.get(table).get) = 1
    if (timeBucket > 0)
      vector(currentModel.vectorSize - timeBucket - reserveFeature + gabToIndex(gap) + 1) = 1
    vector.mkString
  }

  def featureToVector(features: String, currentModel: ModelInfo): String = {
    val vector = new Array[Int](currentModel.vectorSize)
    util.Arrays.fill(vector, 0)
    val aggregateCol = features.split("@")(0).split(",")
    val groupByKeys = features.split("@")(2).split(",")
    val joinKeys = features.split("@")(1).split(",")
    val tables = features.split("@")(3).split(",")
    val gap = features.split("@")(4).toInt
    for (accCol <- aggregateCol) if (currentModel.accessedColToVectorIndex.get(accCol).isDefined)
      vector(currentModel.accessedColToVectorIndex.get(accCol).get) = 1
    for (groupCol <- groupByKeys) if (currentModel.groupByKeyToVectorIndex.get(groupCol).isDefined)
      vector(currentModel.groupByKeyToVectorIndex.get(groupCol).get) = 1
    for (joinCol <- joinKeys) if (currentModel.joinKeyToVectorIndex.get(joinCol).isDefined)
      vector(currentModel.joinKeyToVectorIndex.get(joinCol).get) = 1
    for (table <- tables) if (currentModel.tableToVectorIndex.get(table).isDefined)
      vector(currentModel.tableToVectorIndex.get(table).get) = 1
    if (timeBucket > 0)
      vector(currentModel.vectorSize - timeBucket + gabToIndex(gap) + 1) = 1
    vector.mkString
  }


  def getQueriesText(vecs: String): Seq[String] = {
    /*X*/ (vecs.grouped(currentModel.vectorSize - reserveFeature - timeBucket).toList.flatMap(vectorToQueryString).filter(x => x != null && x._1.size > 1)).toList.sortBy(_._2).map(_._1)
  }

  def X(s: Seq[(String, Int)]): Seq[(String, Int)] = {
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

  def queryToFeature(lpp: LogicalPlan, gap: Int): String = {
    val tableName: HashMap[String, String] = new HashMap()
    tableName.clear()
    updateAttributeName2(lpp, tableName)
    getAggSubQueries(lpp).map(lp => {
      val aggregateCol = definition.Paths.extractFilterColumns(lp).distinct
      val joinKeys = definition.Paths.getJoinConditions(lp).distinct.sortBy(_.toString).filter(!_.contains("userDefinedColumn"))
      val groupByKeys = getGroupByKeys(lp).distinct.sortBy(_.toString).filter(!_.contains("userDefinedColumn"))
      val tables = getTables(lp).distinct.sortBy(_.toString)
      Seq(aggregateCol.mkString(","), joinKeys.mkString(","), groupByKeys.mkString(","), tables.mkString(","),
        (if (timeBucket > 0) gabToIndex(gap) else -1)).mkString("@")
    }).mkString(delimiterVector)
  }



  def vectorToQueryString(vec: String): Seq[(String, Int)] = {
    var accessCols = new ListBuffer[String]()
    var groupBy = new ListBuffer[String]()
    var joinKey = new ListBuffer[String]()
    val queries = new ListBuffer[String]()
    var tables = new ListBuffer[String]()
    var arrivalRate = 0
    var flag = true
    val bits = vec.split("")
    var query = "Select "
    for (index <- (0 to currentModel.vectorSize - reserveFeature - timeBucket - 1)) if (bits(index) == "1") {
      if (currentModel.accessColIndexRange._1 <= index && index <= currentModel.accessColIndexRange._2) {
        val ac = currentModel.indexToAccessedCol.get(index).get
        accessCols.+=(ac)
        val x = ac.split("\\.")
        futureProjectList.+=(x(0) + "_1." + x(1))
      }
      else if (currentModel.groupByIndexRange._1 <= index && index <= currentModel.groupByIndexRange._2) {
        groupBy.+=(currentModel.indexToGroupByKey.get(index).get)
      }
      else if (currentModel.joinKeyIndexRange._1 <= index && index <= currentModel.joinKeyIndexRange._2) {
        joinKey.+=(currentModel.indexToJoinKey.get(index).get)
      }
      else if (currentModel.tableIndexRange._1 <= index && index <= currentModel.tableIndexRange._2) {
        tables.+=(currentModel.indexToTable.get(index).get)
      }
      else if (currentModel.tableIndexRange._2 < index && index <= currentModel.tableIndexRange._2 + timeBucket) {
        val iii = index - currentModel.tableIndexRange._2
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
    if (accessCols.size == 0 && tables.size == 0)
      return Seq()

    if (joinKey.size == 0) {
      if (groupBy.size > 0) {
        val t = groupBy.map(_.split("\\.")(0)).toSet
        for (x <- t)
          queries.+=("select count(*) from " + x + " group by " + groupBy.filter(v => v.split("\\.")(0).equals(x)).mkString(","))
      }
      else {
        if (tables.size > 0)
          queries += ("select count(*) from " + tables(0))
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
      val tables = joinKey.flatMap(_.split("=")).map(_.split("\\.")(0)).toSet
      if (groupBy.filter(v => tables.contains(v.split("\\.")(0))).size == 0)
        queries.+=("select count(*) from " + tables.mkString(",") + " where " + joinKey.mkString(" and ") + " ")
      else
        queries.+=("select count(*) from " + tables.mkString(",") + " where " + joinKey.mkString(" and ") + " group by " + groupBy.filter(v => tables.contains(v.split("\\.")(0))).mkString(","))
    }
  //  queries.foreach(println)
  //  println(arrivalRate)
  //  println(".......")

    if (queries.size == 0)
      return Seq()
    queries.map(x => (x, arrivalRate))
  }

  /*def readVectorInfo(): Unit = {
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
  }*/

  def ReadModelInfo() = {
    for (s <- setup) {
      for (m <- 1 to 6000) if (Files.exists(Paths.get(pathToML_Info + s + "_M" + m + tag + "_colIndex"))) {
        val model = new ModelInfo(s, m)
        var lines = Source.fromFile(pathToML_Info + s + "_M" + m + tag + "_colIndex").getLines
        while (lines.hasNext) {
          val entry = (lines.next()).split(delimiterHashMap)
          model.indexToAccessedCol.put(entry(1).toInt, entry(0))
          model.accessedColToVectorIndex.put(entry(0), entry(1).toInt)
        }
        lines = Source.fromFile(pathToML_Info + s + "_M" + m + tag + "_groupIndex").getLines
        while (lines.hasNext) {
          val entry = (lines.next()).split(delimiterHashMap)
          model.indexToGroupByKey.put(entry(1).toInt, entry(0))
          model.groupByKeyToVectorIndex.put(entry(0), entry(1).toInt)
        }
        lines = Source.fromFile(pathToML_Info + s + "_M" + m + tag + "_joinIndex").getLines
        while (lines.hasNext) {
          val entry = (lines.next()).split(delimiterHashMap)
          model.indexToJoinKey.put(entry(1).toInt, entry(0))
          model.joinKeyToVectorIndex.put(entry(0), entry(1).toInt)
        }
        lines = Source.fromFile(pathToML_Info + s + "_M" + m + tag + "_tableIndex").getLines
        while (lines.hasNext) {
          val entry = (lines.next()).split(delimiterHashMap)
          model.vectorSize = entry(1).toInt
          model.indexToTable.put(entry(1).toInt, entry(0))
          model.tableToVectorIndex.put(entry(0), entry(1).toInt)
        }
        model.accessColIndexRange = (0, model.indexToAccessedCol.size - 1)
        model.groupByIndexRange = (model.indexToAccessedCol.size, model.indexToAccessedCol.size + model.indexToGroupByKey.size - 1)
        model.joinKeyIndexRange = (model.indexToAccessedCol.size + model.indexToGroupByKey.size, model.indexToAccessedCol.size + model.indexToGroupByKey.size + model.indexToJoinKey.size - 1)
        model.tableIndexRange = (model.indexToAccessedCol.size + model.indexToGroupByKey.size + model.indexToJoinKey.size, model.indexToAccessedCol.size + model.indexToGroupByKey.size + model.indexToJoinKey.size + model.indexToTable.size - 1)
        model.vectorSize += (1 + reserveFeature + timeBucket)
        var flag = false
        lines = Source.fromFile(pathToML_Info + s + "_M" + m + tag).getLines
        while (lines.hasNext)
          if (lines.next().split(";").size >= 3)
            flag = true
        //var flag2 = false
        //lines = Source.fromFile(pathToML_Info + s + "_M" + m + tag2).getLines
        //while (lines.hasNext)
        //  if (lines.next().split(";").size >= 5)
        //    flag2 = true
        if (model.isValid() && flag /*&& flag2*/ )
          modelInfo.put((s, m), model)
      }
    }
  }

}

/*    queryCNT = 0
  timeTotalRuntime = System.nanoTime() - timeTotalRuntime
  for (query <- queries) if (queryCNT <= testSize) {
   //sparkSession.sqlContext.clearCache()

   // outputOfQuery = ""
   // println(i + "qqqqqqqqqqqqqqqqqq" + query)
   // i += 1

   // checkpointForAppQueryPlanning = System.nanoTime()
   // costModel.addQuery(query._1, query._3)
    val appPhysicalPlan = costModel.suggest()
    checkpointForAppQueryPlanning = (System.nanoTime() - checkpointForAppQueryPlanning) / 1

    for (subqueryAPP <- appPhysicalPlan) {
      var cheapest = changeSynopsesWithScan(prepareForExecution(subqueryAPP, sparkSession))
      if (isExtended)
        cheapest = ExtendProject(cheapest, costModel.getFutureProjectList().distinct)

      var t = System.nanoTime()
      val tt = System.nanoTime()
      executeAndStoreSample(cheapest, sparkSession)
      cheapest = changeSynopsesWithScan(cheapest)
      checkPointForSampling += (System.nanoTime() - t) / 1
      countReusedSample(cheapest)
      t = System.nanoTime()
      //  println(cheapest)
      cheapest.executeCollectPublic().toList.sortBy(_.toString()).foreach(row => {
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
    results += ((query._1, outputOfQuery, checkpointForAppQueryExecution, checkpointForAppQueryPlanning
      , checkPointForSampling, checkAct, checkWare, checkPointForExecution))
    val agg = results.reduce((a, b) => ("", "", a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7, a._8 + b._8))
    println(checkPointForExecution / 1000000000 + "," + agg._8 / 1000000000)
  }
  timeTotalRuntime = System.nanoTime() - timeTotalRuntime
  */
