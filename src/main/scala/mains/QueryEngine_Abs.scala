package mains

import org.apache.spark.sql.catalyst.expressions.codegen.{Predicate => GenPredicate, _}
import definition.Paths.{costOfDistinctSample, costOfJoin, costOfProject, costOfScan, costOfShuffle, costOfUniformSample, costOfUniversalSample, fractionStep, _}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReuseExchange}
import org.apache.spark.sql.execution._
import rules.physical.{ChangeSampleToScan, ExtendProjectList, PushFilterDown}
import java.io._
import java.util.Date
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import costModel.CostModelAbs
import definition.Paths
import operators.physical._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.catalyst.plans.logical.ReturnAnswer
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec

import scala.collection.{Seq, mutable}
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.reflect.io.Directory
import scala.util.Random
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow

abstract class QueryEngine_Abs(name: String, isLocal: Boolean = true) extends Serializable {

  val sparkSession = if (isLocal)
    SparkSession.builder.appName(name).master("local[*]").getOrCreate()
  else
    SparkSession.builder.appName(name).getOrCreate()
  SparkSession.setActiveSession(sparkSession)
  System.setProperty("geospark.global.charset", "utf8")
  sparkSession.sparkContext.setLogLevel("ERROR");
  sparkSession.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  sparkSession.conf.set("spark.driver.maxResultSize", "8g")
  sparkSession.conf.set("spark.sql.codegen.wholeStage", false) // disable codegen
  sparkSession.conf.set("spark.sql.crossJoin.enabled", true)
  var i = 1
  var timeTotalRuntime: Long = 0
  var timeReadNextQuery: Long = 0
  var timeAPPGeneration: Long = 0
  var timePlanSuggestion: Long = 0
  var timeUpdateWarehouse: Long = 0
  var timeUpdateWindowHorizon: Long = 0
  var queries: List[(String, String, Long)] = null
  var confidence = 0.90
  var error = 0.10
  val results = new ListBuffer[(String, String, Long, Long, Long, Long, Long, Long)]()
  var isLRU = true
  var isExtended = true
  var justAPP = false
  var isAdaptive = false
  val futureProjectList = new ListBuffer[String]()

  val zValue = Array.fill[Double](100)(0.70)
  zValue(99) = 2.58
  zValue(98) = 1.96
  zValue(97) = 1.96
  zValue(96) = 1.96
  zValue(95) = 1.96
  zValue(94) = 1.64
  zValue(93) = 1.64
  zValue(92) = 1.64
  zValue(91) = 1.64
  zValue(90) = 1.64
  zValue(89) = 1.44
  zValue(88) = 1.44
  zValue(87) = 1.44
  zValue(86) = 1.44
  zValue(85) = 1.44
  zValue(84) = 1.28
  zValue(83) = 1.28
  zValue(82) = 1.28
  zValue(81) = 1.28
  zValue(80) = 1.28
  zValue(79) = 1.15
  zValue(78) = 1.15
  zValue(77) = 1.15
  zValue(76) = 1.15
  zValue(75) = 1.15
  zValue(74) = 1.04
  zValue(73) = 1.04
  zValue(72) = 1.04
  zValue(71) = 1.04
  zValue(70) = 1.04
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


  def printExactResult(q: String) = {
    val x = sparkSession.experimental.extraStrategies
    val xx = sparkSession.experimental.extraOptimizations
    sparkSession.experimental.extraStrategies = Seq()
    sparkSession.experimental.extraOptimizations = Seq()
    val appPhysicalPlan = getAggSubQueries(sparkSession.sqlContext.sql(q).queryExecution.analyzed)
      .map(pp => sparkSession.sessionState.planner.plan(ReturnAnswer(sparkSession.sessionState.optimizer.execute(pp))).toSeq(0))
    for (cheapest <- appPhysicalPlan)
      prepareForExecution(cheapest, sparkSession).executeCollectPublic().toList.sortBy(_.toString()).foreach(println)
    sparkSession.experimental.extraStrategies = x
    sparkSession.experimental.extraOptimizations = xx

  }

  def execute(costModel: CostModelAbs) = {
    timeTotalRuntime = System.nanoTime()
    queries = loadWorkloadWithIP("skyServer", sparkSession)
    var timeCHK: Long = 0
    var timeExec: Long = 0
    for (query <- queries) if (queryCNT <= testSize) {
      sparkSession.sqlContext.clearCache()
      outputOfQuery = ""
      println(i + "qqqqqqqqqqqqqqqqqq" + query)
      i += 1

      timeCHK = System.nanoTime()
      if (isAdaptive && i > 0 && (i % step == 0 || i % step == stepper)) {
        windowSize = costModel.UpdateWindowHorizon()
        println(windowSize)
      }
      timeUpdateWindowHorizon += System.nanoTime() - timeCHK


      timeCHK = System.nanoTime()
      val future = ReadNextQueries(query._1, query._2, query._3, i)
      timeReadNextQuery += System.nanoTime() - timeCHK

      timeCHK = System.nanoTime()
      costModel.addQuery(query._1, future,futureProjectList.distinct)
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
        // println(cheapest)
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
  }


  def readConfiguration(args: Array[String]): Unit = {
    if (isLocal) {
      val parent = "/home/hamid/QAL/QP/skyServer/"
      pathToQueryLog = parent + "workload/log7"
      pathToML_Info = parent + "ML/"
      pathToTableParquet = parent + "data_parquet/"
      pathToSaveSynopses = parent + "warehouse/" //"/home/hamid/QAL/QP/skyServer/warehouse/"
      windowSize = 10
      futureWindowSize = 5
      maxSpace = 1000
      confidence = 0.99
      error = 0.1
      testSize = 50
      isLRU = false
      isExtended = false
      justAPP = true
      isAdaptive = true
      tag = "processVec_1800Gap_5processMinLength_10000maxQueryRepetition_1featureMinFRQ_20reserveFeature_2007fromYear_20202toYear"
      //tag = "processVec_1800Gap_2processMinLength_10000maxQueryRepetition_1featureMinFRQ_10reserveFeature_2007fromYear"
      featureCNT = 555 //118//365//412 //571 //138
      costOfProject = 1 //2 // args(11).toInt
      costOfScan = 1 //5 // args(12).toInt
      costOfJoin = 10 //6 //args(13).toInt
      costOfShuffle = 10 //args(14).toInt
      costOfUniformSample = 10 // args(15).toInt
      costOfUniversalSample = 15 // args(16).toInt
      costOfDistinctSample = 25 //args(17).toInt
    }
    else {
      val parent = args(0) //"/home/hamid/QAL/QP/skyServer/"
      pathToQueryLog = parent + "workload/" + args(1)
      pathToML_Info = parent + "ML/"
      pathToTableParquet = parent + "data_parquet/"
      pathToSaveSynopses = parent + "warehouse/" //"/home/hamid/QAL/QP/skyServer/warehouse/"
      windowSize = args(2).toInt
      futureWindowSize = args(2).toInt
      maxSpace = args(3).toInt
      confidence = args(4).toDouble
      error = args(5).toDouble
      testSize = args(6).toInt
      tag = args(7)
      featureCNT = args(8).toInt
      isLRU = args(9).toBoolean
      isExtended = args(10).toBoolean
      justAPP = args(11).toBoolean
      isAdaptive = args(12).toBoolean
      if (args.size > 13) {
        costOfProject = args(13).toInt
        costOfScan = args(14).toInt
        costOfJoin = args(15).toInt
        costOfShuffle = args(16).toInt
        costOfUniformSample = args(17).toInt
        costOfUniversalSample = args(18).toInt
        costOfDistinctSample = args(19).toInt
        costOfProject = 1 //2 // args(11).toInt
        costOfScan = 1 //5 // args(12).toInt
        costOfJoin = 10 //6 //args(13).toInt
        costOfShuffle = 10 //args(14).toInt
        costOfUniformSample = 40 // args(15).toInt
        costOfUniversalSample = 45 // args(16).toInt
        costOfDistinctSample = 25 //args(17).toInt
      }
      else {
        val r = scala.util.Random
        r.setSeed(System.nanoTime() / 1000)
        costOfProject = 1 + r.nextInt(10)
        costOfScan = 1 + r.nextInt(10)
        costOfJoin = 1 + r.nextInt(10)
        costOfShuffle = 1 + r.nextInt(10)
        costOfUniformSample = 1 + r.nextInt(10)
        costOfUniversalSample = 1 + r.nextInt(10)
        costOfDistinctSample = 1 + r.nextInt(10)
      }
    }
    if (new java.io.File(parentDir + "synopsesSize.ser").exists)
      new ObjectInputStream(new FileInputStream(parentDir + "synopsesSize.ser")).readObject.asInstanceOf[mutable.HashMap[String, Long]].foreach((a) => synopsesSize.put(a._1, a._2))

  }

  def ReadNextQueries(query: String, ip: String, epoch: Long, queryIndex: Int): Seq[String]

  def flush(): Unit = {
    (new File(pathToSaveSynopses)).listFiles.filter(_.getName.contains(".obj")).foreach(Directory(_).deleteRecursively())
    //  (new File(pathToSketches)).listFiles.foreach(x => x.delete())

    sketchesMaterialized.clear()
    warehouseParquetNameToSize.clear()
    sampleToOutput.clear()
    SynopsesToParquetName.clear()
    ParquetNameToSynopses.clear()
    parquetNameToHeader.clear()
    lastUsedOfParquetSample.clear()
    counterForQueryRow = 0
    outputOfQuery = ""
    lastUsedCounter = 0
    numberOfSynopsesReuse = 0
    numberOfGeneratedSynopses = 0
    numberOfRemovedSynopses = 0
    numberOfExecutedSubQuery = 0
    counterNumberOfGeneratedRow = 0
    timeForUpdateWarehouse = 0
    timeForSampleConstruction = 0
    timeTotal = 0
    timeForSubQueryExecution = 0
    timeForTokenizing = 0
  }


  def toStringTree(pp: SparkPlan, intent: String = ""): String = pp match {
    case s: ScaleAggregateSampleExec =>
      "" + toStringTree(s.child, intent)
    case s: FilterExec =>
      "" + toStringTree(s.child, intent) // intent + "FFFF\n" + pp.children.map(child => toStringTree(child, intent + "     ")).mkString("")
    case s: ProjectExec =>
      "" + toStringTree(s.child, intent) //intent + "PPPP(" + getHeaderOfOutput(p.projectList.map(_.asInstanceOf[Attribute])) + ")\n" + toStringTree(p.child, intent + "     ")
    case s: RDDScanExec =>
      val table = tableName.get(s.output(0).toString().toLowerCase).get.split("\\.")(0).dropRight(2)
      intent + "SSSS(" + table + ")\n"
    case s: SortMergeJoinExec =>
      intent + "JJJJ(" + getHeaderOfOutput(s.leftKeys) + "=" + getHeaderOfOutput(s.rightKeys) + ")\n" + pp.children.map(child => toStringTree(child, intent + "     ")).mkString("")
    case HashAggregateExec(requiredChildDistributionExpressions, groupingExpressions, aggregateExpressions, aggregateAttributes, initialInputBufferOffset, resultExpressions, child: HashAggregateExec) =>
      intent + "AAAA(" + aggToString(aggregateExpressions) + ", GroupBy(" + getHeaderOfOutput(groupingExpressions.map(x => x.find(_.isInstanceOf[AttributeReference]).get.asInstanceOf[Attribute])) + "))\n" + toStringTree(child, intent + "     ")
    case s: HashAggregateExec =>
      "" + toStringTree(s.child, intent)
    case s: UniformSampleExec2 =>
      intent + "UNIF(" + aggToString(s.functions) + ")\n" + toStringTree(s.child, intent + "     ")
    case s: UniversalSampleExec2 =>
      intent + "UNIV(" + aggToString(s.functions) + ")\n" + toStringTree(s.child, intent + "     ")
    case s: DistinctSampleExec2 =>
      intent + "DIST(" + getHeaderOfOutput(s.groupingExpression.map(x => x.find(_.isInstanceOf[AttributeReference]).get.asInstanceOf[Attribute])) + aggToString(s.functions) + ")\n" + toStringTree(s.child, intent + "     ")
  }

  def aggToString(aggregateExpressions: Seq[AggregateExpression]) = aggregateExpressions.map(x => "" + x.aggregateFunction.children.map(p => getAttNameOfAtt(p.find(_.isInstanceOf[Attribute]).map(_.asInstanceOf[Attribute]).getOrElse(null))).mkString("") + "").mkString(",")


  def executeAndStoreSample(pp: SparkPlan, sparkSession: SparkSession): Unit = {
    val queue = new mutable.Queue[SparkPlan]()
    queue.enqueue(pp)
    while (!queue.isEmpty) {
      queue.dequeue() match {
        case s: operators.physical.SampleExec =>
          Random.setSeed(System.nanoTime())
          val synopsisInfo = s.toString()
          val name = "sample" + Random.alphanumeric.filter(_.isLetter).take(20).mkString.toLowerCase
          if (s.output.size == 0) {
            val z = UniformSampleExec2(s.asInstanceOf[UniformSampleExec2].functions, s.asInstanceOf[UniformSampleExec2].confidence, s.asInstanceOf[UniformSampleExec2].error, s.asInstanceOf[UniformSampleExec2].seed,
              ProjectExec(s.find(p => p.output.size != 0).get.output.slice(0, 1), s.child.children(0)))
            parquetNameToHeader.put(name, getHeaderOfOutput(z.output))
            sampleToOutput.put(name, z.output)
            prepareForExecution(z, sparkSession).execute().saveAsObjectFile(pathToSaveSynopses + name + ".obj")
            numberOfGeneratedSynopses += 1
            val size = folderSize(new File(pathToSaveSynopses + name + ".obj"))
            warehouseParquetNameToSize.put(name.toLowerCase, size /*view.rdd.count()*view.schema.map(_.dataType.defaultSize).reduce(_+_)*/)
            ParquetNameToSynopses.put(name, z.toString())
            SynopsesToParquetName.put(z.toString(), name)
            sampleToOutput.put(name, z.output)
            lastUsedCounter += 1
            lastUsedOfParquetSample.put(name, lastUsedCounter)
            parquetNameToHeader.put(name, getHeaderOfOutput(z.output))
            synopsesSize.put(z.toString(), size)
            val oos: ObjectOutputStream = new ObjectOutputStream(new FileOutputStream(parentDir + "synopsesSize.ser"))
            oos.writeObject(synopsesSize)
            oos.close()
            println("stored: " + name + "," + "," + size + "," + z.toString())
            return
          }
          prepareForExecution(s, sparkSession).execute().saveAsObjectFile(pathToSaveSynopses + name + ".obj")
          numberOfGeneratedSynopses += 1
          val size = folderSize(new File(pathToSaveSynopses + name + ".obj"))
          warehouseParquetNameToSize.put(name.toLowerCase, size /*view.rdd.count()*view.schema.map(_.dataType.defaultSize).reduce(_+_)*/)
          ParquetNameToSynopses.put(name, synopsisInfo)
          SynopsesToParquetName.put(synopsisInfo, name)
          sampleToOutput.put(name, s.output)
          lastUsedCounter += 1
          lastUsedOfParquetSample.put(name, lastUsedCounter)
          parquetNameToHeader.put(name, getHeaderOfOutput(s.output))
          synopsesSize.put(s.toString(), size)
          val oos: ObjectOutputStream = new ObjectOutputStream(new FileOutputStream(parentDir + "synopsesSize.ser"))
          oos.writeObject(synopsesSize)
          oos.close()

          println("stored: " + name + "," + "," + size + "," + s.toString())
        case a =>
          a.children.foreach(x => queue.enqueue(x))
      }
    }
  }


  def executeAndStoreSample2(pp: SparkPlan, sparkSession: SparkSession): Unit = {
    val queue = new mutable.Queue[SparkPlan]()
    queue.enqueue(pp)
    while (!queue.isEmpty) {
      queue.dequeue() match {
        case ss: operators.physical.SampleExec =>
          var sampler = ss
          Random.setSeed(System.nanoTime())
          val name = "sample" + Random.alphanumeric.filter(_.isLetter).take(20).mkString.toLowerCase
          var OK = false
          val zv = zValue((sampler.getConfidence * 100).toInt)
          val err = sampler.getError
          while (!OK) {
            var q: RDD[InternalRow] = null
            var synopsisInfo = ""
            if (sampler.output.size == 0) {
              sampler.find(p => p.output.size != 0).get.output.slice(0, 1)
              val z = UniformSampleExec2(sampler.asInstanceOf[UniformSampleExec2].functions, sampler.asInstanceOf[UniformSampleExec2].confidence, sampler.asInstanceOf[UniformSampleExec2].error, sampler.asInstanceOf[UniformSampleExec2].seed,
                ProjectExec(sampler.find(p => p.output.size != 0).get.output.slice(0, 1), sampler.child.children(0)))
              parquetNameToHeader.put(name, getHeaderOfOutput(z.output))
              sampleToOutput.put(name, z.output)
              q = prepareForExecution(z, sparkSession).execute() //.saveAsObjectFile(pathToSaveSynopses + name + ".obj")
            }
            else {
              parquetNameToHeader.put(name, getHeaderOfOutput(sampler.output))
              sampleToOutput.put(name, sampler.output)
              q = prepareForExecution(sampler, sparkSession).execute() //.map(converter(_).asInstanceOf[Row])
            }
            synopsisInfo = sampler.toString()
            println(sampler.output)
            /*val info = if(synopsisInfo.split(delimiterSynopsisFileNameAtt)(6)=="") "sum(ww___ww#)" else synopsisInfo.split(delimiterSynopsisFileNameAtt)(6)
            val index = sampler.output.zipWithIndex.find(_._1.name.equalsIgnoreCase(info.substring(4, info.indexOf('#')).replace("(",""))).get._2
            val ttype = sampler.output(index).dataType
            val isFiltered = pp.find(_.isInstanceOf[FilterExec])
            val qq = if (false /*isFiltered.isDefined*/) {
              val condition = isFiltered.get.asInstanceOf[FilterExec].condition
              val converter = CatalystTypeConverters.createToScalaConverter(sampler.schema)
              q = prepareForExecution(sampler, sparkSession).execute()
              q.mapPartitionsWithIndex { (index, iter) =>
                val predicate = newPredicate(condition, sampler.output)
                predicate.initialize(0)
                iter.filter(predicate.eval(_))
              }.map(converter(_).asInstanceOf[Row])
            } else {
              val converter = CatalystTypeConverters.createToScalaConverter(sampler.schema)
              q.map(converter(_).asInstanceOf[Row])
            }
            OK = if (sampler.isInstanceOf[DistinctSampleExec2]) {
              val groupByKey = sampler.output.zipWithIndex.find(_._1.name.equals(sampler.asInstanceOf[DistinctSampleExec2].groupingExpression(0).name)).map(x => (x._1.dataType, x._2)).get
              val info = qq.map(x => {
                val w = x.getDouble(x.length - 1)
                val v = x.getAs[ttype.type](index).toString.toDouble
                (x.getAs[groupByKey._1.type](groupByKey._2), (v, (w) * (w - 1) * math.pow(v, 2)))
              }).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).map(y => (y._1, (y._2._1, math.sqrt(y._2._2)))).map(x => {
                if ((zv * x._2._2) / x._2._1 < err) (true, 1) else (false, 1)
              }).reduceByKey(_ + _).sortBy(_._1).collect()
              if ((info.size == 1 && info(0)._1) || (info.size == 2 && info(0)._2 / (info(0)._2 + info(1)._2).toDouble < err))
                true
              else
                false
            }
            else {
              val info = qq.map(x => {
                val w = x.getDouble(x.length - 1)
                val v = x.getAs[ttype.type](index).toString.toDouble
                (v, (w) * (w - 1) * math.pow(v, 2))
              }).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
              if ((zv * info._2) / info._1 < err) true else false
            }
            sampler = if (!OK) sampler.transform({
              case u@UniformSampleExec2(functions, confidence, error, seed, child, frac) =>
                if (frac + fractionStep <= 1.0)
                  UniformSampleExec2(functions, confidence, error, seed, child, frac + fractionStep)
                else
                  UniformSampleExec2(functions, confidence, error, seed, child, 1.0)
              case d@DistinctSampleExec2(functions, confidence, error, seed, groupingExpression, child, frac) =>
                if (frac + fractionStep <= 1.0)
                  DistinctSampleExec2(functions, confidence, error, seed, groupingExpression, child, frac + fractionStep)
                else
                  DistinctSampleExec2(functions, confidence, error, seed, groupingExpression, child, 1.0)
              case u@UniversalSampleExec2(functions, confidence, error, seed, joinKey, child, frac) =>
                if (frac + fractionStep <= 1.0)
                  UniversalSampleExec2(functions, confidence, error, seed, joinKey, child, frac + fractionStep)
                else
                  UniversalSampleExec2(functions, confidence, error, seed, joinKey, child, 1.0)
            }).asInstanceOf[SampleExec]
            else sampler
            */
            //if (OK) {
            q.saveAsObjectFile(pathToSaveSynopses + name + ".obj")
            ParquetNameToSynopses.put(name, synopsisInfo)
            SynopsesToParquetName.put(synopsisInfo, name)
            //}
            println("Again")
            OK = true
          }
          // prepareForExecution(s, sparkSession).executeCollectPublic().reverse.take(10).foreach(println)
          numberOfGeneratedSynopses += 1
          warehouseParquetNameToSize.put(name.toLowerCase, folderSize(new File(pathToSaveSynopses + name + ".obj")) /*view.rdd.count()*view.schema.map(_.dataType.defaultSize).reduce(_+_)*/)
          // warehouseParquetNameToRow.put(name.toLowerCase, temp.count()/10000)
          lastUsedCounter += 1
          lastUsedOfParquetSample.put(name, lastUsedCounter)
          println("stored: " + name + "," + sampler.toString() + "," + warehouseParquetNameToSize(name.toLowerCase()))
        case a =>
          a.children.foreach(x => queue.enqueue(x))
      }
    }
  }


  def newPredicate(expression: Expression, inputSchema: Seq[Attribute]): GenPredicate = {
    GeneratePredicate.generate(expression, inputSchema)
  }

  def loadTables(sparkSession: SparkSession): Unit = {
    (new File(pathToTableParquet)).listFiles.filter(_.getName.contains(".parquet")).foreach(table => {
      val view = sparkSession.read.parquet(table.getAbsolutePath) //.sample(.2)//.repartition(1).withColumn("ww___ww", org.apache.spark.sql.functions.lit(1.0));
      sparkSession.sqlContext.createDataFrame(view.rdd, view.schema).createOrReplaceTempView(table.getName.split("\\.")(0).toLowerCase);
      Paths.tableToCount.put(table.getName.split("\\.")(0).toLowerCase, view.count() / 10000)
    })
  }

  def loadWorkloadWithIP(bench: String, sparkSession: SparkSession): List[(String, String, Long)] = {
    var temp: ListBuffer[String] = ListBuffer();
    if (bench.equals("skyServer")) {
      import sparkSession.implicits._
      sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "false")
        .option("inferSchema", "true").option("delimiter", ";").option("nullValue", "null").schema(logSchema)
        .load(pathToQueryLog).filter("statement is not null and clientIP is not null")
        .filter(row => row.getAs[String]("statement").trim.length > 0 && row.getAs[String]("clientIP").trim.length > 0)
        .filter(row => !row.getAs[String]("statement").contains("zoonospec"))
        .select("clientIP", "yy", "mm", "dd", "hh", "mi", "ss", "statement")
        .map(x => (x.getAs[String](7), x.getAs[String](0), new Date(x.getAs[Int](1), x.getAs[Int](2), x.getAs[Int](3)
          , x.getAs[Int](4), x.getAs[Int](5), x.getAs[Int](6)).getTime / 1000)).collect().take(definition.Paths.testSize).toList
    }
    else
      throw new Exception("Invalid input data benchmark")
  }

  def loadWorkload(bench: String, sparkSession: SparkSession): List[String] = {
    var temp: ListBuffer[String] = ListBuffer();
    if (bench.equals("skyServer")) {
      import sparkSession.implicits._
      sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "false")
        .option("inferSchema", "true").option("delimiter", ";").option("nullValue", "null").schema(logSchema)
        .load(pathToQueryLog).filter("statement is not null and clientIP is not null")
        .filter(row => row.getAs[String]("statement").trim.length > 0 && row.getAs[String]("clientIP").trim.length > 0)
        .select("statement").map(_.getAs[String](0)).collect().toList
    }
    else if (bench.equals("atoka") || bench.equals("test") || bench.equals("tpch")) {
      val src = Source.fromFile(pathToQueryLog).getLines
      src.take(1).next
      for (l <- src)
        temp.+=(l.split(';')(0))
      temp.toList
    }
    else
      throw new Exception("Invalid input data benchmark")
  }


  def printReport() = {
    // (query, out, totalT, planTime, samplingT, execT, warehouseTime,execT)
    // val agg = results.reduce((a, b) => ("", "", a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7, a._8 + b._8))

    var fw = new FileWriter("res", true)
    fw.write(Seq(name, maxSpace, windowSize, timeTotalRuntime / 1000000000, (timeUpdateWindowHorizon + timeReadNextQuery + timeAPPGeneration + timePlanSuggestion + timeUpdateWarehouse) / 1000000000, timeUpdateWindowHorizon / 1000000000, timeReadNextQuery / 1000000000, timeAPPGeneration / 1000000000, timePlanSuggestion / 1000000000, timeUpdateWarehouse / 1000000000, numberOfGeneratedSynopses, numberOfSynopsesReuse, numberOfRemovedSynopses, pathToQueryLog.split("/").last, queryCNT, confidence, error, isLRU, isExtended, justAPP, costOfProject, costOfScan, costOfJoin, costOfShuffle, costOfUniformSample, costOfUniversalSample, costOfDistinctSample).mkString("\t\t") + "\n")
    fw.close()
    var writer = new PrintWriter(new File(this.name + "_" + windowSize + "_" + maxSpace + "_" + pathToQueryLog.split("/").last + "_" + justAPP + "_" + confidence + "_" + error))
    //fw = new FileWriter(this.name+"_"+windowSize+"_"+maxSpace+"_"+pathToQueryLog.split("/").last+"_"+justAPP+"_"+confidence+"_"+error, false)
    // results.foreach(X => writer.println((X._8).toString))
    writer.close()
    System.out.println(Seq(name, maxSpace, windowSize, timeTotalRuntime / 1000000000, (timeUpdateWindowHorizon + timeReadNextQuery + timeAPPGeneration + timePlanSuggestion + timeUpdateWarehouse) / 1000000000, timeUpdateWindowHorizon / 1000000000, timeReadNextQuery / 1000000000, timeAPPGeneration / 1000000000, timePlanSuggestion / 1000000000, timeUpdateWarehouse / 1000000000, numberOfGeneratedSynopses, numberOfSynopsesReuse, numberOfRemovedSynopses, pathToQueryLog.split("/").last, queryCNT, confidence, error, isLRU, isExtended, justAPP, costOfProject, costOfScan, costOfJoin, costOfShuffle, costOfUniformSample, costOfUniversalSample, costOfDistinctSample).mkString("\t\t"))
    /*  println("Engine: " + name)
      println("Total query: " + queryCNT)
      println("Number of generated rows: " + counterNumberOfGeneratedRow)
      println("Total execution time: " + agg._3)
      println("Total planning/wr time: " + (agg._4 + agg._7))
      println("Total execution-planning/wr: " + (agg._3 - (agg._4 + agg._7)))
      println("Number of generated synopses: " + numberOfGeneratedSynopses)
      println("Number of reused synopses: " + numberOfSynopsesReuse)
      println("Number of removed synopses: " + numberOfRemovedSynopses)
      println("Average query execution time: " + agg._3.toDouble / queryCNT.toDouble)
      println("Average query planing time: " + agg._4.toDouble / queryCNT.toDouble)
      println("Average sampling time: " + agg._5.toDouble / queryCNT.toDouble)
      println("Average plan execution time: " + agg._6.toDouble / queryCNT.toDouble)
      println("Average warehouse update time: " + agg._7.toDouble / queryCNT.toDouble)
      println("query\t\t out\t\t totalT\t\t planTime\t\t samplingT\t\t execT\t\t warehouseTime")
      // results.foreach(a => println(a._3 + "\t" + a._4 + "\t" + a._5 + "\t" + a._6 + "\t" + a._7 + "\t" + a._2 + "\t" + a._1))*/
    SynopsesToParquetName.toSeq.sortBy(_._1).map(x => (x._1, x._2, warehouseParquetNameToSize.get(x._2))).foreach(println)
  }

  def changeSynopsesWithScan(plan: SparkPlan): SparkPlan = ruleOfSynopsesToScan.foldLeft(plan) {
    case (sp, rule) => rule.apply(sp)
  }

  def ruleOfSynopsesToScan: Seq[Rule[SparkPlan]] = Seq(ChangeSampleToScan(sparkSession))

  def prepareForExecution(plan: SparkPlan, sparkSession: SparkSession): SparkPlan = {
    preparations(sparkSession).foldLeft(plan) {
      case (sp, rule) => rule.apply(sp)
    }
  }

  def ExtendProject(plan: SparkPlan, pl: Seq[String]): SparkPlan = ruleOfExtendingProjectList(pl).foldLeft(plan) {
    case (sp, rule) => rule.apply(sp)
  }

  def ruleOfExtendingProjectList(pl: Seq[String]): Seq[Rule[SparkPlan]] = Seq(ExtendProjectList(pl))

  def preparations(sparkSession: SparkSession): Seq[Rule[SparkPlan]] = Seq(
    PlanSubqueries(sparkSession),
    EnsureRequirements(sparkSession.sessionState.conf),
    CollapseCodegenStages(sparkSession.sessionState.conf),
    PushFilterDown(),
    ReuseExchange(sparkSession.sessionState.conf),
    ReuseSubquery(sparkSession.sessionState.conf)
  )

  def countReusedSample(pp: SparkPlan): Unit = pp match {
    case RDDScanExec(output, rdd, name, outputPartitioning, outputOrdering) =>
      if (name.contains("sample"))
        numberOfSynopsesReuse += 1
    case a =>
      a.children.foreach(countReusedSample)
  }

  def tokenizeQuery(query: String) = {
    var confidence = 0.0
    var error = 0.0
    val tokens = query.split(delimiterToken)
    var dataProfileTable = ""
    var quantileCol = ""
    var quantilePart = 0
    var binningCol = ""
    var binningPart = 0
    var binningStart = 0.0
    var binningEnd = 0.0
    var table = ""
    var tempQuery = ""
    for (i <- 0 to tokens.size - 1)
      if (tokens(i).equalsIgnoreCase("confidence")) {
        confidence = tokens(i + 1).toInt
        tokens(i) = ""
        tokens(i + 1) = ""
      } else if (tokens(i).equalsIgnoreCase("error")) {
        error = tokens(i + 1).toInt
        tokens(i) = ""
        tokens(i + 1) = ""
      }
      else if (tokens(i).equalsIgnoreCase("dataProfile"))
        dataProfileTable = tokens(i + 1)
      else if (tokens(i).compareToIgnoreCase("quantile(") == 0 || tokens(i).compareToIgnoreCase("quantile") == 0 || tokens(i).contains("quantile(")) {
        val att = query.substring(query.indexOf("(") + 1, query.indexOf(")")).split(",")
        quantileCol = att(0)
        quantilePart = att(1).toInt
        tempQuery = "select " + quantileCol + " " + tokens.slice(tokens.indexOf("from"), tokens.indexOf("confidence")).mkString(" ")
      }
      else if (tokens(i).compareToIgnoreCase("binning(") == 0 || tokens(i).compareToIgnoreCase("binning") == 0 || tokens(i).contains("binning(")) {
        val att = query.substring(query.indexOf("(") + 1, query.indexOf(")")).split(",")

        if (att.length == 4) {
          binningCol = att(0)
          binningStart = att(1).toDouble
          binningEnd = att(2).toDouble
          binningPart = att(3).toInt
        }
        else {
          binningCol = att(0)
          binningPart = att(1).toInt
          binningStart = 0
          binningEnd = 0
        }
        tempQuery = "select " + binningCol + " " + tokens.slice(tokens.indexOf("from"), tokens.indexOf("confidence")).mkString(" ")
      }
      else if (tokens(i).equalsIgnoreCase("from"))
        table = tokens(i + 1)
    if (confidence < 0 || confidence > 100 || error < 0 || error > 100)
      throw new Exception("Invalid error and confidence")
    confidence /= 100
    error /= 100
    (tokens.mkString(" "), confidence, error, dataProfileTable, quantileCol, quantilePart.toInt, binningCol, binningPart.toInt
      , binningStart, binningEnd, table, tempQuery)
  }

}

/*//val converter = CatalystTypeConverters.createToScalaConverter(s.schema)
            //val dfOfSample = sparkSession.createDataFrame(prepareForExecution(s, sparkSession).execute().map(converter(_).asInstanceOf[Row]), s.schema)
            //dfOfSample.write.format("parquet").save(pathToSaveSynopses + name + ".parquet");
            //dfOfSample.createOrReplaceTempView(name)
            // prepareForExecution(s, sparkSession).execute().collect().foreach(x=> println(x.getDouble(x.numFields-1)))
            */