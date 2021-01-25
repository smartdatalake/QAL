//-XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:StartFlightRecording=duration=30s,filename=QAL_recording.jfr
import java.io.{BufferedReader, File, FileOutputStream, FileReader, FilenameFilter, PrintWriter}
import java.net.ServerSocket
import definition.{Paths, TableDefs}
import extraSQL.extraSQLOperators
import org.apache.spark.sql.catalyst.{AliasIdentifier, CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project, ReturnAnswer, Sort, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{CollapseCodegenStages, FilterExec, LogicalRDD, PlanLater, PlanSubqueries, ProjectExec, RDDScanExec, ReuseSubquery, SparkPlan}
import rules.logical.{ApproximateInjector, pushFilterUp}
import rules.physical.{ChangeSampleToScan, SampleTransformation, ScaleAggregateSampleExec}
import org.apache.spark.sql.catalyst.analysis._

import scala.collection.{Seq, mutable}
import scala.collection.mutable.ListBuffer
import scala.io.Source
import java.{lang, util}
import operators.physical.{DistinctSampleExec2, SampleExec, UniformSampleExec2, UniformSampleExec2WithoutCI, UniversalSampleExec2}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReuseExchange}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}

import scala.reflect.io.Directory
import definition.Paths._
import main.{costOfPlan, numberOfExecutedSubQuery}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.functions.col
import org.apache.spark.util.Utils

import scala.util.Random
object main {

  val sparkSession = SparkSession.builder
    .appName("AAQP")
    .master("local[*]")
    .getOrCreate();
  val tableCounter = new mutable.HashMap[String, Int]()
  var mapRDDScanRowCNT: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()
  var numberOfExecutedSubQuery = 0
  val threshold = 1000
  var numberOfRemovedSynopses = 0;

  def main(args: Array[String]): Unit = {

    SparkSession.setActiveSession(sparkSession)
    System.setProperty("geospark.global.charset", "utf8")
    sparkSession.sparkContext.setLogLevel("ERROR");
    sparkSession.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkSession.conf.set("spark.driver.maxResultSize", "8g")
    sparkSession.conf.set("spark.sql.codegen.wholeStage", false); // disable codegen
    sparkSession.conf.set("spark.sql.crossJoin.enabled", true)

    val (bench, benchDir, dataDir) = analyzeArgs(args);
    loadTables(sparkSession, bench, dataDir)
    mapRDDScanRowCNT = readRDDScanRowCNT(dataDir)

    val queries = queryWorkload(bench, benchDir)
    val (extraOpt, extraStr) = setRules()
    //    val server = new ServerSocket(4545)
    // println("Server initialized:")
    // while (true) {
    sparkSession.experimental.extraOptimizations = Seq(new ApproximateInjector(0.0, 0.0, seed), new pushFilterUp);
    sparkSession.experimental.extraStrategies = extraStr;
    var outString = ""
    timeTotal = System.nanoTime()
    for (i <- start to start + testSize) {
      val query = queries(i)
      // println(query)
      val (query_code, confidence, error, dataProfileTable, quantileCol, quantilePart, binningCol, binningPart
      , binningStart, binningEnd, table, tempQuery) = tokenizeQuery(query)
      if (quantileCol != "") {
        outString = extraSQLOperators.execQuantile(sparkSession, tempQuery, table, quantileCol, quantilePart, confidence, error, seed)
      } else if (binningCol != "")
        outString = extraSQLOperators.execBinning(sparkSession, table, binningCol, binningPart, binningStart, binningEnd, confidence, error, seed)
      else if (dataProfileTable != "")
        outString = extraSQLOperators.execDataProfile(sparkSession, dataProfileTable, confidence, error, seed)
      else {
        val subQueries = getAggSubQueries(sparkSession.sqlContext.sql(query_code).queryExecution.analyzed)
        outputOfQuery = ""
        for (subQuery <- subQueries) {
          //println(subQuery)
          // val rawPlans = enumerateRawPlanWithJoin(subQuery)
          // val logicalPlans = rawPlans.map(x => sparkSession.sessionState.optimizer.execute(x))
          // val physicalPlans = logicalPlans.flatMap(x => sparkSession.sessionState.planner.plan(ReturnAnswer(x)))
          // val costOfPhysicalPlan = physicalPlans.map(x => (x, costOfPlan(x, Seq()))).sortBy(_._2._2)
          // costOfPhysicalPlan.foreach(println)
          //  println("cheapest plan before execution preparation:")
          // println(costOfPhysicalPlan(0)._1)

          //choose the best approximate physical plan and create related synopses, presently, the lowest costed plan
          //////////////////////////////////////////////////////////////////////////////////////////////////////////
          val checkpointForSampleConstruction = System.nanoTime()
          updateAttributeName(subQuery, new mutable.HashMap[String, Int]())
          val pp = sparkSession.sessionState.planner.plan(ReturnAnswer(sparkSession.sessionState.optimizer.execute(subQuery))).toList(0)
          var cheapestPhysicalPlan = changeSynopsesWithScan(pp)
          executeAndStoreSample(cheapestPhysicalPlan)
          executeAndStoreSketch(cheapestPhysicalPlan)
          cheapestPhysicalPlan = changeSynopsesWithScan(cheapestPhysicalPlan)
          cheapestPhysicalPlan = prepareForExecution(cheapestPhysicalPlan)
          timeForSampleConstruction += (System.nanoTime() - checkpointForSampleConstruction)
          //////////////////////////////////////////////////////////////////////////////////////////////////////////

          //execute the best approximate physical plan with generated synopses
          //////////////////////////////////////////////////////////////////////////////////////////////////////////
          val checkpointForSubQueryExecution = System.nanoTime()
          countReusedSample(cheapestPhysicalPlan)
          cheapestPhysicalPlan.executeCollectPublic().toList.foreach(row => {
            outputOfQuery += row.toString()
            counterNumberOfRowGenerated += 1
          })
          timeForSubQueryExecution += (System.nanoTime() - checkpointForSubQueryExecution)
          numberOfExecutedSubQuery += 1
          //////////////////////////////////////////////////////////////////////////////////////////////////////////

          //update warehouse, LRU or window based
          //////////////////////////////////////////////////////////////////////////////////////////////////////////
          val checkpointForWarehouseUpdate = System.nanoTime()
          if (LRUorWindowBased)
            updateWareHouseLRU
          else
            updateWarehouse(queries.slice(i, i + windowSize))
          timeForUpdateWarehouse += (System.nanoTime() - checkpointForWarehouseUpdate)
          //////////////////////////////////////////////////////////////////////////////////////////////////////////

          tableName.clear()
        }
        // println(counterForQueryRow + "," + (System.nanoTime() - t) / 1000000000)
      }
    }

    /*  lastUsedOfParquetSample.foreach(println)
        tableToCount.foreach(println)
        warehouseParquetNameToSize.foreach(println)
        ParquetNameToSynopses.foreach(println)
        SynopsesToParquetName.foreach(println)
        parquetNameToHeader.foreach(println)*/
    println(LRUorWindowBased.toString + "," + maxSpace + "," + windowSize + "," + fraction
      + "," + (System.nanoTime() - timeTotal) / 1000000000 + "," + timeForSubQueryExecution / 1000000000
      + "," + timeForSampleConstruction / 1000000000 + ","
      + timeForUpdateWarehouse / 1000000000 + "," + numberOfRemovedSynopses
      + "," + numberOfGeneratedSynopses + "," + numberOfSynopsesReuse + "," + numberOfExecutedSubQuery + ","
      + counterNumberOfRowGenerated)
    flush()
  }

  def executeAndStoreSketch(pp: SparkPlan): Unit = {
    val queue = new mutable.Queue[SparkPlan]()
    queue.enqueue(pp)
    while (!queue.isEmpty) {
      queue.dequeue() match {
        case s: operators.physical.SketchExec
          if (!sketchesMaterialized.contains(s.toString())) =>
          val synopsisInfo = s.toString()
          sketchesMaterialized.put(synopsisInfo, s.createSketch())
          numberOfGeneratedSynopses += 1
          lastUsedCounter += 1
          lastUsedOfParquetSample.put(synopsisInfo, lastUsedCounter)
        case a =>
          a.children.foreach(x => queue.enqueue(x))
      }
    }
  }


  def executeAndStoreSample(pp: SparkPlan): Unit = {
    val queue = new mutable.Queue[SparkPlan]()
    queue.enqueue(pp)
    while (!queue.isEmpty) {
      queue.dequeue() match {
        case s: operators.physical.SampleExec =>
          Random.setSeed(System.nanoTime())
          val synopsisInfo = s.toString()
          val name = "sample" + Random.alphanumeric.filter(_.isLetter).take(20).mkString.toLowerCase
          if (s.output.size == 0) {
            //  println("Errror: {" + s.toString() + "} output of sample is not defined because of projectList==[]")
            return
          }
          try {
            //  println(StructType(s.schema.toList.map(x=>new StructField("x",x.dataType,x.nullable,x.metadata))))
            val dfOfSample = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(s.executeCollectPublic()), s.schema)
            dfOfSample.write.format("parquet").save(pathToSaveSynopses + name + ".parquet");
            dfOfSample.createOrReplaceTempView(name)
            numberOfGeneratedSynopses += 1
            warehouseParquetNameToSize.put(name.toLowerCase, folderSize(new File(pathToSaveSynopses + name + ".parquet")) /*view.rdd.count()*view.schema.map(_.dataType.defaultSize).reduce(_+_)*/)
            ParquetNameToSynopses.put(name, synopsisInfo)
            SynopsesToParquetName.put(synopsisInfo, name)
            lastUsedCounter += 1
            lastUsedOfParquetSample.put(name, lastUsedCounter)
            parquetNameToHeader.put(name, getHeaderOfOutput(s.output))
            //  println("stored: " + name + "," + s.toString())
          }
          catch {
            case e: Exception =>
             System.err.println("Errrror: " + name + "  " + s.toString())
          }
        case a =>
          a.children.foreach(x => queue.enqueue(x))
      }
    }
  }

  def countReusedSample(pp: SparkPlan): Unit = pp match {
    case RDDScanExec(output, rdd, name, outputPartitioning, outputOrdering) =>
      if (name.contains("sample"))
        numberOfSynopsesReuse += 1
    case a =>
      a.children.foreach(countReusedSample)
  }

  def convertSampleTextToParquet(sparkSession: SparkSession) = {
    val folder = (new File(pathToSaveSynopses)).listFiles.filter(_.isDirectory)
    for (i <- 0 to folder.size - 1) {
      val name = folder(i).getName.toLowerCase
      if (!name.contains(".parquet") && !folder.find(_.getName == name + ".parquet").isDefined) {
        val f = new File(pathToSaveSynopses + name)
        val filenames = f.listFiles(new FilenameFilter() {
          override def accept(dir: File, name: String): Boolean = name.startsWith("part-")
        })
        try {
          var theUnion: DataFrame = null
          for (file <- filenames) {
            val d = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("delimiter", ",").load(file.getPath)
            if (d.columns.size != 0) {
              if (theUnion == null) theUnion = d
              else theUnion = theUnion.union(d)
            }
          }
          theUnion.toDF(parquetNameToHeader.getOrElse(name, "null").split(delimiterParquetColumn): _*).write.format("parquet").save(pathToSaveSynopses + name + ".parquet");
          val view = sparkSession.read.parquet(pathToSaveSynopses + name + ".parquet")
          sparkSession.sqlContext.createDataFrame(view.rdd, view.schema).createOrReplaceTempView(name)
          warehouseParquetNameToSize.put(name.toLowerCase, folderSize(new File(folder(i).getAbsoluteFile + ".parquet")) /*view.rdd.count()*view.schema.map(_.dataType.defaultSize).reduce(_+_)*/)
          numberOfGeneratedSynopses += 1
          val directory = new Directory(new File(folder(i).getAbsolutePath))
          directory.deleteRecursively()
        }
        catch {
          case e: Exception =>
            System.err.println(e)
            val directory = new Directory(new File(folder(i).getAbsolutePath))
            directory.deleteRecursively()
            SynopsesToParquetName.remove(ParquetNameToSynopses.getOrElse(name, "null"))
            ParquetNameToSynopses.remove(name)
            parquetNameToHeader.remove(name)
            lastUsedOfParquetSample.remove(name)
        }
      }
    }
  }
  def updateWarehouse(future: Seq[String]): Unit = {
    val warehouseSynopsesToSize = warehouseParquetNameToSize.map(x => (ParquetNameToSynopses(x._1), x._2))
    if (warehouseSynopsesToSize.size == 0 || warehouseSynopsesToSize.reduce((a, b) => (null, a._2 + b._2))._2 <= maxSpace)
      return Unit
    // future approximate physical plans
    val rawPlansPerQuery = future.flatMap(query =>
      getAggSubQueries(sparkSession.sqlContext.sql(tokenizeQuery(query)._1).queryExecution.analyzed)
    )
    rawPlansPerQuery.foreach(x => updateAttributeName(x, new mutable.HashMap[String, Int]()))
    val rawPlansPerQueryWithJoins = rawPlansPerQuery.map(enumerateRawPlanWithJoin)
    val logicalPlansPerQuery = rawPlansPerQueryWithJoins.map(x => x.map(sparkSession.sessionState.optimizer.execute(_)))
    val physicalPlansPerQuery = logicalPlansPerQuery.map(x => x.flatMap(y => sparkSession.sessionState.planner.plan(ReturnAnswer(y))))

    val candidateSynopses = new ListBuffer[(String, Long)]()
    var candidateSynopsesSize: Long = 0
    val p = warehouseSynopsesToSize.map(synopsisInWarehouse => (synopsisInWarehouse, physicalPlansPerQuery.map(physicalPlans =>
      physicalPlans.map(physicalPlan => {
        //  println(synopsisInWarehouse+"\n"+physicalPlan+"\n"+(costOfPlan(physicalPlan, candidateSynopses)._2 - costOfPlan(physicalPlan, candidateSynopses ++ Seq(synopsisInWarehouse))._2))
        costOfPlan(physicalPlan, candidateSynopses)._2 - costOfPlan(physicalPlan, candidateSynopses ++ Seq(synopsisInWarehouse))._2
      }).reduce((A1, A2) => if (A1 > A2) A1 else A2)
    ).reduce(_ + _))).map(x => (x._1._1, x._2)).toList.sortBy(_._2).reverse
    //println("best is:\n"+bestSynopsis)
    // create a list of synopses for keeping, gradually
    var index = 0
    var bestSynopsis = p(index)
    var bestSynopsisSize = warehouseSynopsesToSize.getOrElse(bestSynopsis._1, 0.toLong)
    val removeSynopses = new mutable.HashSet[String]()
    while (index < warehouseSynopsesToSize.size - 1) {
      if (candidateSynopsesSize + bestSynopsisSize < maxSpace) {
        candidateSynopsesSize += bestSynopsisSize
        index += 1
        bestSynopsis = p(index)
        bestSynopsisSize = warehouseSynopsesToSize.getOrElse(bestSynopsis._1, 0.toLong)
      }
      else {
        removeSynopses.add(bestSynopsis._1)
        index += 1
        bestSynopsis = p(index)
        bestSynopsisSize = warehouseSynopsesToSize.getOrElse(bestSynopsis._1, 0.toLong)
      }
    }
    removeSynopses.foreach(x => {
      val parquetName = SynopsesToParquetName.getOrElse(x, "null")
      (Directory(new File(pathToSaveSynopses + parquetName + ".parquet"))).deleteRecursively()
      warehouseParquetNameToSize.remove(parquetName)
      SynopsesToParquetName.remove(x)
      ParquetNameToSynopses.remove(parquetName)
      parquetNameToHeader.remove(parquetName)
      lastUsedOfParquetSample.remove(parquetName)
      //println("removed:    " + x)
      numberOfRemovedSynopses += 1
    })
  }

  /*  def updateWarehouse(future: Seq[String]): Unit = {
    val warehouseSynopsesToSize = warehouseParquetNameToSize.map(x => (ParquetNameToSynopses(x._1), x._2))
    if (warehouseSynopsesToSize.reduce((a, b) => (null, a._2 + b._2))._2 <= maxSpace)
      return Unit
    // future approximate physical plans
    val rawPlansPerQuery = future.flatMap(query =>
      getAggSubQueries(sparkSession.sqlContext.sql(tokenizeQuery(query)._1).queryExecution.analyzed)
    )
    rawPlansPerQuery.foreach(x => updateAttributeName(x, new mutable.HashMap[String, Int]()))
    val rawPlansPerQueryWithJoins = rawPlansPerQuery.map(enumerateRawPlanWithJoin)
    val logicalPlansPerQuery = rawPlansPerQueryWithJoins.map(x => x.map(sparkSession.sessionState.optimizer.execute(_)))
    val physicalPlansPerQuery = logicalPlansPerQuery.map(x => x.flatMap(y => sparkSession.sessionState.planner.plan(ReturnAnswer(y))))

    val candidateSynopses = new ListBuffer[(String, Long)]()
    var candidateSynopsesSize: Long = 0
    var bestSynopsis = warehouseSynopsesToSize.map(synopsisInWarehouse => (synopsisInWarehouse, physicalPlansPerQuery.map(physicalPlans =>
      physicalPlans.map(physicalPlan => {
      //  println(synopsisInWarehouse+"\n"+physicalPlan+"\n"+(costOfPlan(physicalPlan, candidateSynopses)._2 - costOfPlan(physicalPlan, candidateSynopses ++ Seq(synopsisInWarehouse))._2))
        costOfPlan(physicalPlan, candidateSynopses)._2 - costOfPlan(physicalPlan, candidateSynopses ++ Seq(synopsisInWarehouse))._2}).reduce((A1, A2) => if (A1 > A2) A1 else A2)
    ).reduce(_ + _))).map(x => (x._1._1, x._2)).reduce((A1, A2) => if (A1._2 > A2._2) A1 else A2)
    //println("best is:\n"+bestSynopsis)
    // create a list of synopses for keeping, gradually
    var bestSynopsisSize = warehouseSynopsesToSize.getOrElse(bestSynopsis._1, 0.toLong)
    val removeSynopses = new mutable.HashSet[String]()
    while (warehouseSynopsesToSize.size != 1) {
      if (candidateSynopsesSize + bestSynopsisSize < maxSpace) {
        candidateSynopsesSize += bestSynopsisSize
        candidateSynopses += (bestSynopsis)
        warehouseSynopsesToSize.remove(bestSynopsis._1)
        bestSynopsis = warehouseSynopsesToSize.map(synopsisInWarehouse => (synopsisInWarehouse, physicalPlansPerQuery.map(physicalPlans =>
          physicalPlans.map(physicalPlan => {
         //   println(synopsisInWarehouse+"\n"+physicalPlan+"\n"+(costOfPlan(physicalPlan, candidateSynopses)._2 - costOfPlan(physicalPlan, candidateSynopses ++ Seq(synopsisInWarehouse))._2))
            costOfPlan(physicalPlan, candidateSynopses)._2 - costOfPlan(physicalPlan, candidateSynopses ++ Seq(synopsisInWarehouse))._2}).reduce((A1, A2) => if (A1 > A2) A1 else A2)
        ).reduce(_ + _))).map(x => (x._1._1, x._2)).reduce((A1, A2) => if (A1._2 > A2._2) A1 else A2)
        println("best is:\n"+bestSynopsis)
        bestSynopsisSize = warehouseSynopsesToSize.getOrElse(bestSynopsis._1, 0.toLong)
      }
      else {
        removeSynopses.add(bestSynopsis._1)
        warehouseSynopsesToSize.remove(bestSynopsis._1)
        bestSynopsis = warehouseSynopsesToSize.map(synopsisInWarehouse => (synopsisInWarehouse, physicalPlansPerQuery.map(physicalPlans =>
          physicalPlans.map(physicalPlan => {
        //    println(synopsisInWarehouse+"\n"+physicalPlan+"\n"+(costOfPlan(physicalPlan, candidateSynopses)._2 - costOfPlan(physicalPlan, candidateSynopses ++ Seq(synopsisInWarehouse))._2))
            costOfPlan(physicalPlan, candidateSynopses)._2 - costOfPlan(physicalPlan, candidateSynopses ++ Seq(synopsisInWarehouse))._2}).reduce((A1, A2) => if (A1 > A2) A1 else A2)
        ).reduce(_ + _))).map(x => (x._1._1, x._2)).reduce((A1, A2) => if (A1._2 > A2._2) A1 else A2)
        println("best is:\n"+bestSynopsis)
        bestSynopsisSize = warehouseSynopsesToSize.getOrElse(bestSynopsis._1, 0.toLong)
      }
    }
    removeSynopses.foreach(x => {
      val parquetName = SynopsesToParquetName.getOrElse(x, "null")
      (Directory(new File(pathToSaveSynopses + parquetName + ".parquet"))).deleteRecursively()
      warehouseParquetNameToSize.remove(parquetName)
      SynopsesToParquetName.remove(x)
      ParquetNameToSynopses.remove(parquetName)
      parquetNameToHeader.remove(parquetName)
      lastUsedOfParquetSample.remove(parquetName)
      println("removed:    " + x)
      numberOfRemovedSynopses += 1
    })
  }*/

  def costOfPlan(pp: SparkPlan, synopsesCost: Seq[(String, Long)]): (Long, Long) = pp match { //(#row,CostOfPlan)
    case FilterExec(filters, child) =>
      val (inputSize, childCost) = costOfPlan(child, synopsesCost)
      ((filterRatio * inputSize).toLong, costOfFilter * inputSize + childCost)
    case ProjectExec(projectList, child) =>
      val (inputSize, childCost) = costOfPlan(child, synopsesCost)
      val projectRatio = projectList.size / child.output.size.toDouble
      ((projectRatio * inputSize).toLong, costOfProject * inputSize + childCost)
    case SortMergeJoinExec(a, b, c, d, left, right) =>
      val (leftInputSize, leftChildCost) = costOfPlan(left, synopsesCost)
      val (rightInputSize, rightChildCost) = costOfPlan(right, synopsesCost)
      val outputSize = leftInputSize * rightInputSize
      (outputSize, costOfJoin * outputSize + leftChildCost + rightChildCost)
    case l@RDDScanExec(a, b, s, d, g) =>
      val size: Long = mapRDDScanRowCNT.getOrElse(definition.Paths.getTableColumnsName(l.output).mkString(";").toLowerCase, -1)
      if (size == -1)
        throw new Exception("The size does not exist: " + l.toString())
      (size, costOfScan * size)
    case s@UniformSampleExec2(functions, confidence, error, seed, child) =>
      synopsesCost.find(x => isMoreAccurate(s, x._1)).map(x => (x._2, costOfScan * x._2)).getOrElse({
        val (inputSize, childCost) = costOfPlan(child, synopsesCost)
        ((s.fraction * inputSize).toLong, costOfUniformSample * inputSize + childCost)
      })
    case s@DistinctSampleExec2(functions, confidence, error, seed, groupingExpression, child) =>
      synopsesCost.find(x => isMoreAccurate(s, x._1)).map(x => (x._2, costOfScan * x._2)).getOrElse({
        val (inputSize, childCost) = costOfPlan(child, synopsesCost)
        ((s.fraction * inputSize).toLong, costOfDistinctSample * inputSize + childCost)
      })
    case s@UniversalSampleExec2(functions, confidence, error, seed, joinKeys, child) =>
      synopsesCost.find(x => isMoreAccurate(s, x._1)).map(x => (x._2, costOfScan * x._2)).getOrElse({
        val (inputSize, childCost) = costOfPlan(child, synopsesCost)
        ((s.fraction * inputSize).toLong, costOfUniversalSample * inputSize + childCost)
      })
    case s@UniformSampleExec2WithoutCI(seed, child) =>
      synopsesCost.find(x => isMoreAccurate(s, x._1)).map(x => (x._2, costOfScan * x._2)).getOrElse({
        val (inputSize, childCost) = costOfPlan(child, synopsesCost)
        ((s.fraction * inputSize).toLong, costOfUniformWithoutCISample * inputSize + childCost)
      })
    case s@ScaleAggregateSampleExec(confidence, error, seed, fraction, resultsExpression, child) =>
      val (rowCNT, childCost) = child.map(x => costOfPlan(x, synopsesCost)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
      (rowCNT, costOfScale * rowCNT + childCost)
    case h@HashAggregateExec(requiredChildDistributionExpressions, groupingExpressions, aggregateExpressions, aggregateAttributes, initialInputBufferOffset, resultExpressions, child) =>
      val (rowCNT, childCost) = costOfPlan(child, synopsesCost)
      (rowCNT, HashAggregate * rowCNT + childCost)
    case h@SortAggregateExec(requiredChildDistributionExpressions, groupingExpressions, aggregateExpressions, aggregateAttributes, initialInputBufferOffset, resultExpressions, child) =>
      val (rowCNT, childCost) = costOfPlan(child, synopsesCost)
      (rowCNT, SortAggregate * rowCNT + childCost)
    case _ =>
      throw new Exception("No cost is defined for the node")
  }


  def updateWareHouseLRU: Unit = {
    val warehouseSynopsesToSize = warehouseParquetNameToSize.map(x => (ParquetNameToSynopses(x._1), x._2))
    if (warehouseSynopsesToSize.size == 0 || warehouseSynopsesToSize.reduce((a, b) => (null, a._2 + b._2))._2 <= maxSpace)
      return Unit
    var candidateSynopsesSize: Long = 0
    val p = lastUsedOfParquetSample.toList.sortBy(_._2).reverse
    var index = 0
    var bestSynopsis = p(index)
    var bestSynopsisSize = warehouseSynopsesToSize.getOrElse(ParquetNameToSynopses(bestSynopsis._1), 0.toLong)
    val removeSynopses = new mutable.HashSet[String]()
    while (index < warehouseSynopsesToSize.size - 1) {
      if (candidateSynopsesSize + bestSynopsisSize < maxSpace) {
        candidateSynopsesSize += bestSynopsisSize
        index += 1
        bestSynopsis = p(index)
        bestSynopsisSize = warehouseSynopsesToSize.getOrElse(ParquetNameToSynopses(bestSynopsis._1), 0.toLong)
      }
      else {
        removeSynopses.add(bestSynopsis._1)
        index += 1
        bestSynopsis = p(index)
        bestSynopsisSize = warehouseSynopsesToSize.getOrElse(ParquetNameToSynopses(bestSynopsis._1), 0.toLong)
      }
    }
    removeSynopses.foreach(x => {
      (Directory(new File(pathToSaveSynopses + x + ".parquet"))).deleteRecursively()
      warehouseParquetNameToSize.remove(x)
      SynopsesToParquetName.remove(ParquetNameToSynopses.getOrElse(x, "null"))
      ParquetNameToSynopses.remove(x)
      parquetNameToHeader.remove(x)
      lastUsedOfParquetSample.remove(x)
      System.err.println("removed:    " + x)
      numberOfRemovedSynopses += 1
    })
  }


  def flush(): Unit = {
    (new File(pathToSaveSynopses)).listFiles.foreach(Directory(_).deleteRecursively())
    warehouseParquetNameToSize.clear()
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
    counterNumberOfRowGenerated = 0
    timeForUpdateWarehouse = 0
    timeForSampleConstruction = 0
    timeTotal = 0
    timeForSubQueryExecution = 0
    timeForTokenizing = 0

    System.err.println("Flushed everything")
  }

  def countTable(lp: LogicalPlan): Unit = lp match {
    case node@org.apache.spark.sql.catalyst.analysis.UnresolvedRelation(table) =>
      tableCounter.update(table.table, tableCounter.getOrElse(table.table, 0) + 1)
    case t =>
      t.children.foreach(child => countTable(child))
  }

  def getAggSubQueries(lp: LogicalPlan): Seq[LogicalPlan] = lp match {

    case a@Aggregate(groupingExpressions, aggregateExpressions, child) =>
      Seq(a) ++ getAggSubQueries(child)
    case a =>
      a.children.flatMap(child => getAggSubQueries(child))
  }

  def enumerateRawPlanWithJoin(rawPlan: LogicalPlan): Seq[LogicalPlan] = {
    return Seq(rawPlan)
    var rootTemp: ListBuffer[LogicalPlan] = new ListBuffer[LogicalPlan]
    val subQueries = new ListBuffer[SubqueryAlias]()
    val queue = new mutable.Queue[LogicalPlan]()
    var joinConditions: ListBuffer[BinaryExpression] = new ListBuffer[BinaryExpression]()
    queue.enqueue(rawPlan)
    while (!queue.isEmpty) {
      val t = queue.dequeue()
      t match {
        case SubqueryAlias(name, child) =>
          subQueries.+=(t.asInstanceOf[SubqueryAlias])
        case a@Aggregate(groupingExpressions, aggregateExpressions, child) =>
          queue.enqueue(child)
          rootTemp.+=(Aggregate(groupingExpressions, aggregateExpressions, child))
        case Filter(conditions, child) =>
          joinConditions.++=(getJoinConditions(conditions))
          queue.enqueue(child)
          rootTemp.+=(Filter(conditions, rawPlan.children(0).children(0)))
        case org.apache.spark.sql.catalyst.plans.logical.Join(
        left: LogicalPlan, right: LogicalPlan, joinType: JoinType,
        condition: Option[Expression]) =>
          queue.enqueue(left)
          queue.enqueue(right)
          if (condition.isDefined)
            joinConditions.+=(condition.get.asInstanceOf[BinaryExpression])
        case Project(p, c) =>
          queue.enqueue(c)
          rootTemp.+=(Project(p, rawPlan.children(0).children(0)))
        case Sort(order: Seq[SortOrder], global: Boolean, child: LogicalPlan) =>
          queue.enqueue(child)
          rootTemp += Sort(order, global, rawPlan.children(0).children(0))
        case u@UnresolvedRelation(table) =>
          subQueries.+=(SubqueryAlias(AliasIdentifier(table.table), u))

        case _ =>
          throw new Exception("new logical raw node")
      }
    }
    var allPossibleJoinOrders = new ListBuffer[Seq[LogicalPlan]]
    allPossibleJoinOrders.+=(Seq())
    allPossibleJoinOrders.+=(subQueries)
    for (i <- 2 to subQueries.size) {
      val temp = new ListBuffer[Join]
      for (j <- (1 to i - 1))
        if (j == i - j) {
          for (l <- 0 to allPossibleJoinOrders(j).size - 1)
            for (r <- l + 1 to allPossibleJoinOrders(j).size - 1)
              if (!hasTheSameSubquery(allPossibleJoinOrders(j)(l), allPossibleJoinOrders(j)(r))
                && IsJoinable(allPossibleJoinOrders(j)(l), allPossibleJoinOrders(j)(r), joinConditions))
                temp.+=:(Join(allPossibleJoinOrders(j)(l), allPossibleJoinOrders(j)(r), org.apache.spark.sql.catalyst.plans.Inner, Some(chooseConditionFor(allPossibleJoinOrders(j)(l), allPossibleJoinOrders(j)(r), joinConditions))))
        } else if (j < i - j) {
          for (left <- allPossibleJoinOrders(j))
            for (right <- allPossibleJoinOrders(i - j))
              if (!hasTheSameSubquery(left, right) && IsJoinable(left, right, joinConditions)) {
                val jj = (Join(left, right, org.apache.spark.sql.catalyst.plans.Inner, Some(chooseConditionFor(left, right, joinConditions))))
                //if(!isCartesianProduct(jj))
                temp.+=:(jj)
              }
        }
      allPossibleJoinOrders.+=(temp)
    }
    var plans = new ListBuffer[LogicalPlan]()
    for (j <- 0 to allPossibleJoinOrders(subQueries.size).size - 1) {
      plans.+=(allPossibleJoinOrders(subQueries.size)(j))
      for (i <- rootTemp.size - 1 to 0 by -1) {
        rootTemp(i) match {
          case Aggregate(groupingExpressions, aggregateExpressions, child) =>
            plans(j) = Aggregate(groupingExpressions, aggregateExpressions, plans(j))
          case Filter(condition, child) =>
            plans(j) = Filter(condition, plans(j))
          case Project(p, c) =>
            plans(j) = Project(p, plans(j))
          case Sort(o, g, c) =>
            plans(j) = Sort(o, g, plans(j))
          case _ =>
            throw new Exception("new logical node")
        }
      }
    }
    plans
  }

  def enumerateRawPlanWithJoin(queryCode: String): Seq[LogicalPlan] = enumerateRawPlanWithJoin(sparkSession.sqlContext.sql(queryCode).queryExecution.analyzed)

  def chooseConditionFor(lp1: LogicalPlan, lp2: LogicalPlan, joinConditions: ListBuffer[BinaryExpression]): Expression = {
    for (i <- 0 to joinConditions.length - 1) {
      val lJoinkey = Set(joinConditions(i).left.find(_.isInstanceOf[AttributeReference]).get.toString().toLowerCase)
      val rJoinkey = Set(joinConditions(i).right.find(_.isInstanceOf[AttributeReference]).get.toString().toLowerCase)
      if ((lJoinkey.subsetOf(lp1.output.map(_.toAttribute.toString().toLowerCase).toSet) && rJoinkey.subsetOf(lp2.output.map(_.toAttribute.toString().toLowerCase).toSet))
        || (lJoinkey.subsetOf(lp2.output.map(_.toAttribute.toString().toLowerCase).toSet) && rJoinkey.subsetOf(lp1.output.map(_.toAttribute.toString().toLowerCase).toSet)))
        return joinConditions(i).asInstanceOf[Expression]
    }
    throw new Exception("Cannot find any condition to join the two tables")
  }

  def getJoinConditions(exp: Expression): Seq[BinaryExpression] = exp match {
    case a@And(left, right) =>
      return (getJoinConditions(left) ++ getJoinConditions(right))
    case o@Or(left, right) =>
      return (getJoinConditions(left) ++ getJoinConditions(right))
    case b@BinaryOperator(left, right) =>
      if (left.find(_.isInstanceOf[AttributeReference]).isDefined && right.find(_.isInstanceOf[AttributeReference]).isDefined)
        return Seq(b)
      Seq()
    case a =>
      Seq()
  }

  def IsJoinable(lp1: LogicalPlan, lp2: LogicalPlan, joinConditions: ListBuffer[BinaryExpression]): Boolean = {
    for (i <- 0 to joinConditions.length - 1) {
      val lJoinkey = Set(joinConditions(i).left.find(_.isInstanceOf[AttributeReference]).get.toString().toLowerCase)
      val rJoinkey = Set(joinConditions(i).right.find(_.isInstanceOf[AttributeReference]).get.toString().toLowerCase)
      if ((lJoinkey.subsetOf(lp1.output.map(_.toAttribute.toString().toLowerCase).toSet) && rJoinkey.subsetOf(lp2.output.map(_.toAttribute.toString().toLowerCase).toSet))
        || (lJoinkey.subsetOf(lp2.output.map(_.toAttribute.toString().toLowerCase).toSet) && rJoinkey.subsetOf(lp1.output.map(_.toAttribute.toString().toLowerCase).toSet)))
        return true
    }
    false
  }

  def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  def isCartesianProduct(join: Join): Boolean = {
    val conditions = join.condition.map(splitConjunctivePredicates).getOrElse(Nil)

    conditions match {
      case Seq(Literal.FalseLiteral) | Seq(Literal(null, BooleanType)) => false
      case _ => !conditions.map(_.references).exists(refs =>
        refs.exists(join.left.outputSet.contains) && refs.exists(join.right.outputSet.contains))
    }
  }

  def hasTheSameSubquery(plan1: LogicalPlan, plan2: LogicalPlan): Boolean = {
    val queue = new mutable.Queue[LogicalPlan]()
    val subquery = new ListBuffer[AliasIdentifier]
    queue.enqueue(plan2)
    while (!queue.isEmpty) {
      val t = queue.dequeue()
      t match {
        case y@SubqueryAlias(name, child) =>
          subquery.+=(name)
        case _ =>
          t.children.foreach(x => queue.enqueue(x))
      }
    }
    hasSubquery(plan1, subquery)
  }

  def hasSubquery(plan: LogicalPlan, query: Seq[AliasIdentifier]): Boolean = {

    plan match {
      case SubqueryAlias(name: AliasIdentifier, child: LogicalPlan) =>
        if (query.contains(name))
          return true
      case _ =>
        plan.children.foreach(x => if (hasSubquery(x, query) == true) return true)
    }
    false
  }

  def getTableNameToSampleParquet(in: SparkPlan, logicalToTable: mutable.HashMap[String, String], map: mutable.HashMap[String, String]): Unit = {
    in match {
      case sample@UniformSampleExec2(functions, confidence, error, seed, child@RDDScanExec(output, rdd, outputPartitioning, outputOrderingSeq, isStreaming)) =>
        map.put(sample.toString() + "_parquet", logicalToTable.getOrElse(output.map(_.name).slice(0, 15).mkString(""), "Missing logical plan to table!!"))
      case sample@UniformSampleExec2WithoutCI(seed, child@RDDScanExec(output, rdd, outputPartitioning, outputOrderingSeq, isStreaming)) =>
        map.put(sample.toString() + "_parquet", logicalToTable.getOrElse(output.map(_.name).slice(0, 15).mkString(""), "Missing logical plan to table!!"))
      case sample@UniversalSampleExec2(functions, confidence, error, seed, joinKey, child@RDDScanExec(output, rdd, outputPartitioning, outputOrderingSeq, isStreaming)) =>
        map.put(sample.toString() + "_parquet", logicalToTable.getOrElse(output.map(_.name).slice(0, 15).mkString(""), "Missing logical plan to table!!"))
      case sample@DistinctSampleExec2(functions, confidence, error, seed, groupingExpression, child@RDDScanExec(output, rdd, outputPartitioning, outputOrderingSeq, isStreaming)) =>
        map.put(sample.toString() + "_parquet", logicalToTable.getOrElse(output.map(_.name).slice(0, 15).mkString(""), "Missing logical plan to table!!"))
      case _ =>
        in.children.map(child => getTableNameToSampleParquet(child, logicalToTable, map))
    }
  }


  /*def costOfPlan(pp: SparkPlan, synopsesCost: Seq[(String, Long)]): (Long, Long) = pp match { //(#row,CostOfPlan)
    case FilterExec(filters, child) =>
      val (rowCNT, childCost) = costOfPlan(child, synopsesCost)
      ((filterRatio * rowCNT).toLong, (costOfFilter * rowCNT) + childCost)
    case ProjectExec(projectList, child) =>
      val (rowCNT, childCost) = costOfPlan(child, synopsesCost)
      (rowCNT, costOfProject * rowCNT + childCost)
    case SortMergeJoinExec(a, b, c, d, left, right) =>
      val (leftInputRowCNT, leftChildCost) = costOfPlan(left, synopsesCost)
      val (rightInputRowCNT, rightChildCost) = costOfPlan(right, synopsesCost)
      val outputRowCNT = leftInputRowCNT * rightInputRowCNT
      (outputRowCNT, costOfJoin * outputRowCNT + leftChildCost + rightChildCost)
    case l@RDDScanExec(a, b, s, d, g) =>
      val rowCNT: Long = mapRDDScanRowCNT.getOrElse(definition.Paths.getTableColumnsName(l.output).mkString(";").toLowerCase, -1)
      if (rowCNT == -1)
        throw new Exception("The size does not exist: " + l.toString())
      (rowCNT, costOfScan * rowCNT * getSizeOfAtt(a))
    case s@UniformSampleExec2(functions, confidence, error, seed, child) =>
      synopsesCost.find(x => isMoreAccurate(s, x._1)).map(x => (x._2, costOfScan * x._2)).getOrElse({
        val (inputSize, childCost) = costOfPlan(child, synopsesCost)
        ((s.fraction * inputSize).toLong, costOfUniformSample * inputSize + childCost)
      })
    case s@DistinctSampleExec2(functions, confidence, error, seed, groupingExpression, child) =>
      synopsesCost.find(x => isMoreAccurate(s, x._1)).map(x => (x._2, costOfScan * x._2)).getOrElse({
        val (inputSize, childCost) = costOfPlan(child, synopsesCost)
        ((s.fraction * inputSize).toLong, costOfDistinctSample * inputSize + childCost)
      })
    case s@UniversalSampleExec2(functions, confidence, error, seed, joinKeys, child) =>
      synopsesCost.find(x => isMoreAccurate(s, x._1)).map(x => (x._2, costOfScan * x._2)).getOrElse({
        val (inputSize, childCost) = costOfPlan(child, synopsesCost)
        ((s.fraction * inputSize).toLong, costOfUniversalSample * inputSize + childCost)
      })
    case s@UniformSampleExec2WithoutCI(seed, child) =>
      synopsesCost.find(x => isMoreAccurate(s, x._1)).map(x => (x._2, costOfScan * x._2)).getOrElse({
        val (inputSize, childCost) = costOfPlan(child, synopsesCost)
        ((s.fraction * inputSize).toLong, costOfUniformWithoutCISample * inputSize + childCost)
      })
    case s@ScaleAggregateSampleExec(confidence, error, seed, fraction, resultsExpression, child) =>
      val (rowCNT, childCost) = child.map(x => costOfPlan(x, synopsesCost)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
      (rowCNT, costOfScale * rowCNT + childCost)
    case h@HashAggregateExec(requiredChildDistributionExpressions, groupingExpressions, aggregateExpressions, aggregateAttributes, initialInputBufferOffset, resultExpressions, child) =>
      val (rowCNT, childCost) = costOfPlan(child, synopsesCost)
      (rowCNT, HashAggregate * rowCNT + childCost)
    case h@SortAggregateExec(requiredChildDistributionExpressions, groupingExpressions, aggregateExpressions, aggregateAttributes, initialInputBufferOffset, resultExpressions, child) =>
      val (rowCNT, childCost) = costOfPlan(child, synopsesCost)
      (rowCNT, SortAggregate * rowCNT + childCost)
    case _ =>
      throw new Exception("No cost is defined for the node")
  }*/

  def isMoreAccurate(sampleExec: SampleExec, sampleInf: String): Boolean = sampleExec match {
    case u@UniformSampleExec2(functions, confidence, error, seed, child) =>
      val sampleInfo = sampleInf.split(delimiterSynopsisFileNameAtt)
      if (sampleInfo(0).equals("Uniform") && getHeaderOfOutput(u.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet)
        && sampleInfo(2).toDouble >= confidence && sampleInfo(3).toDouble <= error
      //     && functions.map(_.toString()).toSet.subsetOf(sampleInfo(5).split(delimiterSynopsesColumnName).toSet)
      )
        true else
        false
    case d@DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, child) =>
      val sampleInfo = sampleInf.split(delimiterSynopsisFileNameAtt)
      if (sampleInfo(0).equals("Distinct") && getHeaderOfOutput(d.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet)
        && sampleInfo(2).toDouble >= confidence && sampleInfo(3).toDouble <= error
        //   && functions.map(_.toString()).toSet.subsetOf(sampleInfo(6).split(delimiterSynopsesColumnName).toSet)
        && getAccessedColsOfExpressions(groupingExpressions).toSet.subsetOf(sampleInfo(7).split(delimiterSynopsesColumnName).toSet))
        true else
        false
    case u@UniversalSampleExec2(functions, confidence, error, seed, joinKey, child) =>
      val sampleInfo = sampleInf.split(delimiterSynopsisFileNameAtt)
      if (sampleInfo(0).equals("Universal") && getHeaderOfOutput(u.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet)
        && sampleInfo(2).toDouble >= confidence && sampleInfo(3).toDouble <= error
        //   && functions.map(_.toString()).toSet.subsetOf(sampleInfo(5).split(delimiterSynopsesColumnName).toSet)
        && getAccessedColsOfExpressions(joinKey).toSet.subsetOf(sampleInfo(6).split(delimiterSynopsesColumnName).toSet))
        true else
        false
    case u@UniformSampleExec2WithoutCI(seed, child) =>
      val sampleInfo = sampleInf.split(delimiterSynopsisFileNameAtt)
      if (sampleInfo(0).equals("UniformWithoutCI") && getHeaderOfOutput(u.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet))
        true else
        false
    case _ => false
  }

  def recursiveProcess(in: LogicalPlan, map: mutable.HashMap[String, String]): Unit = {
    in match {
      case SubqueryAlias(name1, child@SubqueryAlias(name2, lr@LogicalRDD(output, rdd, o, p, f))) =>
        map.put(output.map(_.name).slice(0, 15).mkString(""), name2.identifier)
      case _ =>
        in.children.map(child => recursiveProcess(child, map))
    }
  }

  def folderSize(directory: File): Long = {
    var length: Long = 0
    for (file <- directory.listFiles) {
      if (file.isFile) length += file.length
      else length += folderSize(file)
    }
    length / 100000
  }

  def getTableName(s: String): String = {
    val startTableName = s.indexOf(" ")
    val stopTableName = s.indexOf(",");
    if (startTableName < 0 || stopTableName < 0) return null;
    s.substring(startTableName, stopTableName).trim();
  }

  def tokenizeQuery(query: String) = {
    var confidence = 0.0
    var error = 0.0
    val tokens = query.split(" ")
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
        binningCol = att(0)
        binningStart = att(1).toDouble
        binningEnd = att(2).toDouble
        binningPart = att(3).toInt
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

  def loadTables(sparkSession: SparkSession, bench: String, dataDir: String) = {
    if (bench.contains("tpch"))
      TableDefs.load_tpch_tables(sparkSession, dataDir)
    else if (bench.contains("skyServer"))
      TableDefs.load_skyServer_tables(sparkSession, dataDir);
    else if (bench.contains("atoka"))
      TableDefs.load_atoka_tables(sparkSession, dataDir);
    else if (bench.contains("test"))
      TableDefs.load_test_tables(sparkSession, dataDir);
    else if (bench.contains("proteus"))
      TableDefs.load_proteus_tables(sparkSession, dataDir);
    else
      throw new Exception("!!!Invalid dataset!!!")
  }

  def cleanSkyServerQuery(query: String): String = {
    var token = query.split(" ")
    for (i <- (0 to token.size - 1)) {
      if (token(i).equals("as") && token(i + 1).contains("\'"))
        token(i + 1) = token(i + 1).replace("\'", " ")
      if (token(i).equalsIgnoreCase("top")) {
        token(i) = ""
        token(i + 1) = ""
      }
      if (token(i).contains("fgetNearByObjEq")) {
        token(i) = "photoobj"
      }
      if (token(i).equalsIgnoreCase("into") && token(i + 1).contains("db")) {
        token(i) = ""
        token(i + 1) = ""
      }
    }
    token.mkString(" ").replace("stdev(", "avg(").replace("stdev (", "avg (").replaceAll("mydb\\.ring_galaxies_z", "galaxy").replace("ring_galaxies_z", "galaxy")
  }

  def queryWorkload(bench: String, BENCH_DIR: String): List[String] = {
    var temp: ListBuffer[String] = ListBuffer();
    if (bench.equals("skyServer")) {
      var src = Source.fromFile(BENCH_DIR + "queryLog.csv").getLines
      src.take(1).next
      var counter = 0
      val queryLog = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
        .option("inferSchema", "true").option("delimiter", ";").option("nullValue", "null").load(BENCH_DIR + "queryLog.csv");
      // queryLog.filter("rows>0").sort(col("clientIP"),col("seq").asc).select("statement").show()
      for (l <- src) {
        if (!l.contains("round") && !l.contains("petror50") && !l.contains("d.branch_name") && !l.contains("p.redshift") && !l.contains("s.zt") && !l.contains("avg(s.redshift)") && !l.contains("avg(redshift)") && !l.contains("count(8) ra,dec") && !l.contains("objid,ra,dec,dered_u,dered_g,dered_r,dered_i,dered_z") && !l.contains("nik") && !l.contains("specobj,count(*)fr") && !l.contains("count(*) from specobj,count(*)") && !l.contains("specobj,photoobj") && !l.contains("legacyprimary") && !l.contains("galspecextra") && !l.contains("petrorad") && !l.contains("petrorad_r_50") && !l.contains("petrorad_50_r") && !l.contains("petrorad50_r") && !l.contains("radius") && !l.contains("petr50_r") && !l.contains("petror50_r") && !l.contains("g,err_g,r,i,z,g-r") && !l.contains("h83side") && !l.contains("zoo2mainphotoz") && !l.contains("apogeestar") && !l.contains("zoo2mainspecz") && !l.contains("floor(") && !l.contains("round(") && !l.contains("sysdatabases") && !l.contains("information_schema") && !l.contains("logintest") && !l.contains("phototag") && !l.contains("...") && !l.contains("description") && !l.contains("select     count(*),specobjid,survey,mjd,ra,dec,z,psfmag_u,class  from specphotoall  where class='qso' and z>=0  order by z,ra asc") && !l.contains("select type, count(type) from photoobj") && !l.contains("class like type") && !l.contains("tipo") && !l.contains("select count(*) from specobj where ra between 159 and 164 and u > 18 and u-g>2.2") && !l.contains("phototype") && !l.contains("select count(p.ra) from photoobj as p inner join photoobj as gno on p.objid = gno.objid where p.type = 6 and r > 15. and r < 19.") && !l.contains("photozrf") && !l.contains("b.zconf") && !l.contains("specobj where dec>") && !l.contains("+ﬂoor(") && !l.contains("xoriginalid") && !l.contains("select subclass, count(subclass)") && !l.contains("sn_") && !l.contains("crossoriginalid") && !l.contains("modelmag") && !l.contains("select  count(*)  from specobjall s  where s.ra >=") && !l.contains("gz1_agn_mel6002") && !l.contains("eclass") && !l.contains("group by (htmid") && !l.contains("select count(*) from photoobjall") && !l.contains("photoprofile") && !l.contains("peak/snr") && !l.contains("twomass") && !l.contains("masses") && !l.contains(" & ") && !l.contains("count(*)  p.objid") && !l.contains("count(*) p.objid") && !l.contains("count(*), p.objid") && !l.contains("count(*), where") && !l.contains("count(*),where") && !l.contains("count(*)   where") && !l.contains("count(*)  where") && !l.contains("count(*) where") && !l.contains("st.objid") && !l.contains("stellarmassstarformingport") && !l.contains("thingindex") && !l.contains("0x001") && !l.contains("dr9") && !l.contains("fphotoflags") && !l.contains("avg(dec), from") && !l.contains("emissionlinesport") && !l.contains("stellarmasspassiveport") && !l.contains("s.count(z)") && !l.contains("nnisinside") && !l.contains("petromag_u") && !l.contains("insert") && !l.contains("boss_target1") && !l.contains(" photoobj mode = 1") && !l.contains("and count(z)") && !l.contains("gal.extinction_u") && !l.contains("spectroflux_r") && !l.contains("platex") && !l.contains("0x000000000000ffff") && !l.contains("neighbors") && !l.contains("specline") && !l.contains("specclass")) {
          try {
            if (l.split(";")(8).toLong > 0) {
              if (l.split(';')(9).size > 30)
                temp.+=(l.split(';')(9).replace("count(p)", "count(p.type)").replace(" and 30  s.bes", " and 30  and s.bes").replace(" and 25  s.bes", " and 25  and s.bes").replace(" and       and ", " and ").replace(" and      and ", " and ").replace(" and     and ", " and ").replace(" and    and ", " and ").replace(" and   and ", " and ").replace(" and  and ", " and ").replace(" and and ", " and ").replace("from  as photoobjall", "from   photoobjall").replace("\"title\"", " ").replace("p.type \"type\"", "p.type ").replace("\"kirks typo\"", "  ").replace("\"count\"", "  ").replace("\"avg\"", "  ").replace("\"average\"", "  ").replace("\"redshift avg\" ", "  ").replace("\"average redshift from spec table\"", "  ").replace(" eq ", " = ").replace("and avg(petrorad_r) < 18", " ").replace("gobjid", "objid").replace("photoobj 40.97, 13)", "photoobj").replace("sspparams", "sppparams").replace("0.6*round(z/0.6", "z").replace("0.6*round((z+0.3)/0.6 +0.3", "z").replace("0.6*round((z+0.3)/0.6 +0.5", "z").replace("0.6*round((z+0.3)/0.6 -0.5", "z").replace("0.6*round((z+0.3)/0.6 -0.3", "z").replace("0.6*round(z/0.6", "z").replace("-0.3+0.6*round((z+0.3)/0.6", "z").replace("photoobj 10)", "photoobj ").replace("z>=0)", "z>=0").replace("s.zwarning = 0  p.ra between", "s.zwarning = 0 and p.ra between ").replace("order by galaxy", " ").replace("spectrotype", " ").replace("5 and class=galaxy", "5 ").replace(".3  join specobj as s on spectrotype", ".3 ").replace(".3  join specobj as s on s.bestobjid = p.objid", ".3 ").replace(".3  join specobj as s on galspecinfo", ".3 ").replace("grop by galaxy", " ").replace("2 group by galaxy", "2 ").replace("2 and group by type", "2 ").replace("40.2 class like galaxy", "40.2 ").replace("p.g > 2.2", "g > 2.2").replace("betwen", "between").replace("count(type*)", "count(*)").replace("type count(*)", "type, count(*)").replace("type=\"tipo\"", "and type=\"tipo\"").replace("group by type join photoobj as s on s. photo type=name", "group by type").replace("group by type join photoobj as p on p.photo type=name", "group by type").replace("group by type=name", "group by type").replace("group by type join photoobj as s on s. photo type", "group by type").replace("group by type join type as on typen = typen", "group by type").replace("group by type join type on typen = typen", "group by type").replace("group by type = typen", "group by type").replace("group by type join type on typen", "group by type").replace("group by type join photoobj as s on s. photo type=name", "group by type").replace("group by type join type on type-n", "group by type").replace("group by type join type on type.n", "group by type").replace("group by type join photoobj as p on p.photo type=name", "group by type").replace("group by type join photoobj as p on p.type=value", "group by type").replace("u-g 2.2", "u-g > 2.2").replace("group by type join phototype p.fphototypen=p.fphototype", "group by type").replace("group by type join type on fphototype = fphototype", "group by type").replace("group by type join photoobj as p on p.type=value", "group by type").replace("group by type join type on value = value", "group by type").replace("group by type join type on name = name", "group by type").replace("group by type where  join type on name = name", "group by type").replace("group by type join phototype", "group by type").replace("group by type where join type on name = name", "group by type").replace("ra,dec select count(*)", "ra,dec").replace("galaxies", "specobjid").replace("onjid", "objid").replace("avg(aterm_r), avg(kterm_r), avg(airmass_r), avg(aterm_r + kterm_r*airmass_r), gain_r", "avg(aterm_r), avg(kterm_r), avg(airmass_r), avg(aterm_r + kterm_r*airmass_r), avg(gain_r)").replace("select   from field", "field").replace("select   * from field", "field").replace("a.fromvalue, a.tovalue, count(b.z) 'count'", "a.fromvalue, a.tovalue, count(b.z)").replace("bestdr9..", "").replaceAll("\\s{2,}", " ").trim())
            } else
              counter = counter + 1
          }
          catch {
            case e: Exception =>

          }
        }
      }
     // println("number of queries: " + temp.size)
      temp.toList
    }

    else if (bench.equals("atoka") || bench.equals("test") || bench.equals("tpch")) {
      val src = Source.fromFile(BENCH_DIR + "queryLog.csv").getLines
      src.take(1).next
      for (l <- src)
        temp.+=(l.split(';')(0))
      temp.toList
    }
    else
      throw new Exception("Invalid input data benchmark")
  }

  def setRules(): (Seq[Rule[LogicalPlan]], Seq[Strategy]) = {
      (Seq(), Seq(/*SketchPhysicalTransformation,*/  SampleTransformation))
  }

  def analyzeArgs(args: Array[String]): (String,  String,String) = {
    val inputDataBenchmark = args(0);
    val sf = args(1)
    val hdfsOrLocal = args(2)
    val inputDataFormat=args(3)
    fraction = args(4).toDouble
    maxSpace = args(5).toInt * 10
    windowSize = args(6).toInt
    LRUorWindowBased = args(7).toBoolean

    var CURRENTSTATE_DIR = ""
    var PARENT_DIR = ""
    var BENCH_DIR = ""
    if (hdfsOrLocal.equals("local")) {
      PARENT_DIR = "/home/hamid/TASTER/spark-data/" + inputDataBenchmark + "/sf" + sf + "/";
      CURRENTSTATE_DIR = "/home/hamid/TASTER/curstate/"
      BENCH_DIR = "/home/hamid/TASTER/spark-data/" + inputDataBenchmark + "/"
    } else if (hdfsOrLocal.equals("hdfs")) {
      PARENT_DIR = "hdfs://145.100.59.58:9000/TASTER/spark-data/" + inputDataBenchmark + "/sf" + sf + "/";
      CURRENTSTATE_DIR = "/home/hamid/TASTER/curstate/"
      BENCH_DIR = "hdfs://145.100.59.58:9000/TASTER/spark-data/" + inputDataBenchmark + "/"
    }
    else if (hdfsOrLocal.equals("epfl")) {
      PARENT_DIR = "/home/sdlhshah/spark-data/" + inputDataBenchmark + "/"
      CURRENTSTATE_DIR = "/home/sdlhshah/spark-data/curstate/"
      BENCH_DIR = "/home/sdlhshah/spark-data/" + inputDataBenchmark + "/"
    }
    (inputDataBenchmark, BENCH_DIR
      , PARENT_DIR + "data_" + inputDataFormat + "/")
  }

  @throws[Exception]
  def countLines(filename: String): Long = {
    var cnt = 0
    val br = new BufferedReader(new FileReader(filename))
    while ( {
      br.ready
    }) {
      br.readLine
      cnt += 1
    }
    cnt - 1
  }

  /*  def setDistinctSampleCI(in: SparkPlan, sampleRate: mutable.HashSet[Double]): Unit = {
    in match {
      case sample@DistinctSampleExec2(functions, confidence, error, seed, groupingExpression, child) =>
        val aggr = if (functions != null && functions.map(_.toString()).find(x => (x.contains("count(") || x.contains("sum("))).isDefined) 1 else 0
        val ans2 = new util.HashMap[String, Array[Double]]
        val folder = (new File(pathToCIStats)).listFiles.filter(_.isFile)
        var CIStatTable = ""
        for (i <- 0 to folder.size - 1) {
          val br = new BufferedReader(new FileReader(folder(i).getAbsolutePath))
          if (sample.output.map(_.toAttribute.name).mkString(",") == br.readLine) {
            CIStatTable = folder(i).getName.split("\\.").slice(0, 2).mkString("\\.").replace("\\", "")
            while (br.ready) {
              val key = br.readLine
              val vall = br.readLine
              val v = vall.split(",")
              val vd = new Array[Double](v.length)
              for (i <- 0 until v.length) {
                vd(i) = v(i).toDouble
              }
              ans2.put(key, vd)
            }
            br.close()
          }
        }
        val att = if (functions.map(_.aggregateFunction.toString()).take(1)(0).contains("count(1)")) {
          groupingExpression.map(_.name).take(1)(0)
        } else {
          functions.map(x => x.aggregateFunction.children(0).asInstanceOf[AttributeReference].name).take(1)(0)
        }
        sample.fraction = findMinSample(ans2, CIStatTable, att, (confidence * 100).toInt, error, aggr) / 100.0
        sampleRate.add(sample.fraction)
      case sample@UniversalSampleExec2(functions, confidence, error, seed, joinKey, child) =>
        sampleRate.add(sample.fraction)
      case _ =>
        in.children.map(x => setDistinctSampleCI(x, sampleRate))
    }
  }*/

  private def getSignature(filename: String, samplingRate: Int, i: Int, proportionWithin: Int, useAttributeName: Boolean, header: Array[String]) = if (!useAttributeName) filename + "," + proportionWithin + "," + samplingRate + "," + i
  else filename + "," + proportionWithin + "," + samplingRate + "," + header(i)

  private def getSignature(filename: String, samplingRate: Int, attrname: String, proportionWithin: Int) = "SCV.csv" + "," + proportionWithin + "," + samplingRate + "," + attrname

  private def findMinSample(answers: util.HashMap[String, Array[Double]], filename: String, attrname: String, desiredConfidence: Int, desiredError: Double, aggr: Int): Int = { // aggr is 0 for avg or 1 for sum
    val proportionWithin: Int = desiredConfidence
    for (samplingRate <- startSamplingRate to stopSamplingRate by samplingStep) {
      val vall: Array[Double] = answers.get(getSignature(filename, samplingRate, attrname, proportionWithin))
      if (vall != null) {
        if (desiredError > vall(aggr)) {
          if (samplingRate > 30)
            return 30
          return samplingRate
        }
      }
    }
    return 30
  }

  def readRDDScanRowCNT(dataDir: String): mutable.HashMap[String, Long] = {
    val foldersOfParquetTable = new File(dataDir).listFiles.filter(_.isDirectory)
    val tables = sparkSession.sessionState.catalog.listTables("default").map(t => t.table)
    val map = mutable.HashMap[String, Long]()
    foldersOfParquetTable.filter(x => tables.contains(x.getName.split("\\.")(0).toLowerCase)).map(folder => {
      val lRDD = sparkSession.sessionState.catalog.lookupRelation(org.apache.spark.sql.catalyst.TableIdentifier
      (folder.getName.split("\\.")(0), None)).children(0).asInstanceOf[LogicalRDD]
      (RDDScanExec(lRDD.output, lRDD.rdd, "ExistingRDD", lRDD.outputPartitioning, lRDD.outputOrdering), folderSize(folder) /* Paths.tableToCount.getOrElse(x.getName.split("\\.")(0).toLowerCase, -1.toLong).toLong*/ , folder.getName.split("\\.")(0).toLowerCase)
    }).foreach(x => map.put(x._1.output.map(o => x._3 + "." + o.name.split("#")(0)).mkString(";").toLowerCase, x._2))
    map
  }

  def collectPlaceholders(plan: SparkPlan): Seq[(SparkPlan, LogicalPlan)] = {
    plan.collect {
      case placeholder@PlanLater(logicalPlan) => placeholder -> logicalPlan
    }
  }

  def planner(plan: LogicalPlan): Iterator[SparkPlan] = {
    val candidates = sparkSession.sessionState.planner.strategies.iterator.flatMap(_ (plan))
    val plans = candidates.flatMap { candidate =>
      val placeholders = collectPlaceholders(candidate)
      println(candidate)

      if (placeholders.isEmpty) {
        // Take the candidate as is because it does not contain placeholders.
        Iterator(candidate)
      } else {
        // Plan the logical plan marked as [[planLater]] and replace the placeholders.
        placeholders.iterator.foldLeft(Iterator(candidate)) {
          case (candidatesWithPlaceholders, (placeholder, logicalPlan)) =>
            // Plan the logical plan for the placeholder.
            val childPlans = this.planner(logicalPlan)

            candidatesWithPlaceholders.flatMap { candidateWithPlaceholders =>
              childPlans.map { childPlan =>
                // Replace the placeholder by the child plan
                candidateWithPlaceholders.transformUp {
                  case p if p.eq(placeholder) => childPlan
                }
              }
            }
        }
      }
    }
    plans
  }

  def hamidPlanner(plan: LogicalPlan): Iterator[SparkPlan] = {
    val candidates = sparkSession.sessionState.planner.strategies.iterator.flatMap(_ (plan))
    candidates.flatMap(candidate => {
      //  println(candidate)
      val placeholders = collectPlaceholders(candidate)
      if (placeholders.isEmpty) {
        // Take the candidate as is because it does not contain placeholders.
        Iterator(candidate)
      } else {
        placeholders.toList.foreach(println)
        val xxx = placeholders.flatMap(x => hamidPlanner(x._2))
        placeholders.toIterator.flatMap(placeholder => {
          sparkSession.sessionState.planner.strategies.iterator.flatMap(_ (placeholder._2))

          println(candidate)
          val childPlans = hamidPlanner(placeholder._2)
          Seq(candidate).flatMap(p => {
            xxx.flatMap { childPlan =>
              // Replace the placeholder by the child plan
              Iterator(candidate.transformUp {
                case p if p.eq(placeholder) => childPlan
              })
            }
          }).toIterator
        })
      }
    })
  }

  def prepareForExecution(plan: SparkPlan): SparkPlan = {
    preparations.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
  }

  def changeSynopsesWithScan(plan: SparkPlan): SparkPlan = {
    ruleOfSynopsesToScan.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
  }

  def ruleOfSynopsesToScan: Seq[Rule[SparkPlan]] = Seq(
    ChangeSampleToScan(sparkSession)
  )

  def preparations: Seq[Rule[SparkPlan]] = Seq(
    PlanSubqueries(sparkSession),
    EnsureRequirements(sparkSession.sessionState.conf),
    CollapseCodegenStages(sparkSession.sessionState.conf),
    ReuseExchange(sparkSession.sessionState.conf),
    ReuseSubquery(sparkSession.sessionState.conf)
  )

  def updateAttributeName(lp: LogicalPlan, tableFrequency: mutable.HashMap[String, Int]): Unit = lp match {
    case SubqueryAlias(identifier, child@LogicalRDD(output, rdd, outputPartitioning, outputOrdering, isStreaming)) =>
      val att = output.toList
      tableFrequency.put(identifier.identifier, tableFrequency.getOrElse(identifier.identifier, 0) + 1)
      for (i <- 0 to output.size - 1)
        tableName.put(att(i).toAttribute.toString().toLowerCase, identifier.identifier + "_" + tableFrequency.get(identifier.identifier).get)
    case a =>
      a.children.foreach(x => updateAttributeName(x, tableFrequency))
  }

}

//catch {
//  case e: Exception =>
//  val responseDocument = (e.getMessage).getBytes("UTF-8")

//val responseHeader = ("HTTP/1.1 200 OK\r\n" + "Content-Type: text/html; charset=UTF-8\r\n" + "Content-Length: " + responseDocument.length + "\r\n\r\n").getBytes("UTF-8")

//     output.write(responseHeader)
//     output.write(responseDocument)
//     input.close()
//     output.close()
//  }


//    println("Execution time: " + (System.nanoTime() - time) / 100000000)
/*    val responseDocument = (outString).getBytes("UTF-8")
  val responseHeader = ("HTTP/1.1 200 OK\r\n" + "Content-Type: text/html; charset=UTF-8\r\n" + "Content-Length: " + responseDocument.length + "\r\n\r\n").getBytes("UTF-8")
  output.write(responseHeader)
  output.write(responseDocument)
  input.close()
  output.close()
}
catch {
  case e:Exception=>
    val responseDocument = ("{'status':404,'message':\""+e.getMessage+"\"}").getBytes("UTF-8")
    val responseHeader = ("HTTP/1.1 404 FAIL\r\n" + "Content-Type: text/html; charset=UTF-8\r\n" + "Content-Length: " + responseDocument.length + "\r\n\r\n").getBytes("UTF-8")
    output.write(responseHeader)
    output.write(responseDocument)
    input.close()
    output.close()
}*/
//  }
//   catch {
//     case e =>
//       println("errror")
//       println(i)
//       println(queries(i))
//       println(e.toString)
//   }





/*   val filename = schemaFolderPath + folder(i).getName
     var schemaString = ""
     for (line <- Source.fromFile(filename).getLines)
       schemaString = line
     val schema = StructType(schemaString.split("#_").map(fieldName ⇒ {
       val name = fieldName.split("@")(0)
       val ttype = fieldName.split("@")(1)
       ttype match {
         case "string" =>
           StructField(name, StringType, true)
         case "int" =>
           StructField(name, IntegerType, true)
         case "double" =>
           StructField(name, DoubleType, true)
         case "timestamp" =>
           StructField(name, TimestampType, true)
         case "boolean" =>
           StructField(name, BooleanType, true)
         case "long" =>
           StructField(name, LongType, true)
         case _ =>
           throw new Exception("undefined column type!!!")
       }
     }))
     println(schema)*/

//      val view = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "false")
//           .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(path + folder(i).getName)
///         val newColumns = Seq("acheneID","lat","lon" ,"province","isActive","activityStatus","dateOfActivityStart","numberOfEmployees","revenue","EBITDA","balanceSheetClosingDate","flags","ATECO","keywords","taxID","vatID","numberOfBuildings","numberOfPlotsOfLand","numberOfRealEstates","categoryScore","score","updateTime","legalStatus")
//         val df = view.toDF(newColumns:_*)
//   view.columns.foreach(println)
//        view.take(10000).foreach(println)
//       println(view.count())
//       df.write.format("parquet").save(path + folder(i).getName + ";parquet");

/*
*         val candidates2 = sparkSession.sessionState.planner.strategies.iterator.flatMap(_ (lp)).toList(1)
        val placeholders = collectPlaceholders(candidates.toList(1))
        placeholders.flatMap(x=>{

         return x
        })
       val xxx= placeholders.iterator.foldLeft(Iterator(candidates2) ){
          case (candidatesWithPlaceholders, (placeholder, logicalPlan)) =>
            // Plan the logical plan for the placeholder.
            val childPlans = planner(logicalPlan)
            val c=sparkSession.sessionState.planner.strategies.iterator.flatMap(_ (logicalPlan))
            candidatesWithPlaceholders.flatMap { candidateWithPlaceholders =>
              childPlans.map { childPlan =>
                // Replace the placeholder by the child plan
                candidateWithPlaceholders.transformUp {
                  case p if p.eq(placeholder) => childPlan
                }
              }
            }
        }


*
*
* */

/*  setDistinctSampleCI(p, sampleRate)
getTableNameToSampleParquet(p, logicalPlanToTable, sampleParquetToTable)
for (a <- sampleParquetToTable.toList) {
if (sparkSession.sqlContext.tableNames().contains(a._1.toLowerCase)) {
  query_code = query_code.replace(a._2.toUpperCase, a._1)
  sparkSession.experimental.extraStrategies = Seq()
  sparkSession.experimental.extraOptimizations = Seq()
  p = sparkSession.sqlContext.sql(query_code).queryExecution.executedPlan
}
}*/


/*val clientSocket = server.accept()
     val input = clientSocket.getInputStream()
     val output = clientSocket.getOutputStream()
     var query = java.net.URLDecoder.decode(new BufferedReader(new InputStreamReader(input)).readLine, StandardCharsets.UTF_8.name)

     breakable {
       if (!query.contains("GET /QAL?query=")) {
         outString = "{'status':404,'message':\"Wrong REST request!!!\"}"
         val responseDocument = (outString).getBytes("UTF-8")
         val responseHeader = ("HTTP/1.1 404 FAIL\r\n" + "Content-Type: text/html; charset=UTF-8\r\n" + "Content-Length: " + responseDocument.length + "\r\n\r\n").getBytes("UTF-8")
         output.write(responseHeader)
         output.write(responseDocument)
         input.close()
         output.close()
         break()
       }
     }
     query = query.replace("GET /QAL?query=", "").replace(" HTTP/1.1", "")
     try {*/


/*.map(row => {
  var rowString = "{"
  for (i <- 0 to col.size - 1) {
    if (col(i).contains('(') && col(i).split('(')(0).contains("count")) {
      if (row.getLong(i) < partCNT * 2 * minNumberOfOcc) {
        rowString += "\"" + col(i) + "\":" + row.getLong(i) + ","
      } else {
        rowString += "\"" + col(i) + "\":" + (row.getLong(i) * fraction).toLong + ","
      }
    } else if (col(i).contains('(') && col(i).contains("sum")) {
      if (row.getLong(i - 1) < partCNT * 2 * minNumberOfOcc) {
        rowString += "\"" + col(i) + "\":" + row.getDouble(i) + ","
      } else {
        rowString += "\"" + col(i) + "\":" + (row.getDouble(i) * fraction) + ","
      }
    } else {
      if (row.get(i).isInstanceOf[String])
        rowString += "\"" + col(i) + "\":\"" + row.get(i) + "\","
      else
        rowString += "\"" + col(i) + "\":" + row.get(i) + ","
    }
  }
  rowString.dropRight(1) + "}"
}).mkString(",\n") + "]"*/



/*
*
*       else if (true) {
        if (counter % 100 == 0) {
          println(counter)
          tableCounter.toList.sortBy(_._2).reverse.take(100).foreach(println)
        }
        counter = counter + 1
        try {
          val subQueries = getAggSubQueries(sparkSession.sqlContext.sql(query_code).queryExecution.analyzed)
          subQueries.foreach(subQuery => countTable(subQuery))
        }
        catch {
          case _ =>
            println("______________________error_________________")
            println(query_code)
        }
      }*/