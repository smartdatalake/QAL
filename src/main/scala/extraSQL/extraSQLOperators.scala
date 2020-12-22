package extraSQL

import definition.Paths.{ParquetNameToSynopses, SynopsesToParquetName, getHeaderOfOutput, lastUsedCounter, lastUsedOfParquetSample, numberOfGeneratedSynopses, parquetNameToHeader, pathToSaveSynopses, sketchesMaterialized, warehouseParquetNameToSize}
import operators.logical.{Binning, Quantile, UniformSampleWithoutCI}
import operators.physical.{DistinctSampleExec2, UniformSampleExec2, UniformSampleExec2WithoutCI, UniversalSampleExec2}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReturnAnswer, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{LogicalRDD, RDDScanExec, SparkPlan}
import org.apache.spark.sql.types._
import rules.physical.ChangeSampleToScan

import java.io.File
import scala.collection.{Seq, mutable}
import scala.util.Random

object extraSQLOperators {

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

  def execQuantile(sparkSession: SparkSession, tempquery: String, table: String, quantileCol: String, quantilePart: Int
                   , confidence: Double, error: Double, seed: Long): String = {
    var quantileColAtt: AttributeReference = null
    var tempQuery = tempquery
    if (tempQuery.size < 40) {
      var out = "["
      val scan = sparkSession.sqlContext.sql("select * from " + table).queryExecution.optimizedPlan
      for (p <- scan.output.toList)
        if (p.name == quantileCol)
          quantileColAtt = p.asInstanceOf[AttributeReference]
      //sparkSession.experimental.extraStrategies = Seq(SketchPhysicalTransformation)

      var optimizedPhysicalPlans = sparkSession.sessionState.planner.plan(Quantile(quantileColAtt, quantilePart, confidence, error, seed, scan)).toList(1)
      executeAndStoreSketch(optimizedPhysicalPlans)
      executeAndStoreSample(sparkSession,optimizedPhysicalPlans)
      optimizedPhysicalPlans=changeSynopsesWithScan(sparkSession,optimizedPhysicalPlans)
      optimizedPhysicalPlans.executeCollectPublic().foreach(x => out += ("{\"percent\":" + x.get(0) + ",\"value\":" + x.get(1) + "}," + "\n"))
      return out.dropRight(2) + "]"
    }
    val scan = sparkSession.sqlContext.sql(tempQuery).queryExecution.optimizedPlan

    val logicalPlanToTable: mutable.HashMap[String, String] = new mutable.HashMap()
    //recursiveProcess(sparkSession.sqlContext.sql(tempQuery).queryExecution.analyzed, logicalPlanToTable)
    val sampleParquetToTable: mutable.HashMap[String, String] = new mutable.HashMap()
    sparkSession.sessionState.planner.plan(Quantile(quantileColAtt, quantilePart, confidence, error, seed, scan)).toList(0)
    getTableNameToSampleParquet(sparkSession.sessionState.planner.plan(Quantile(quantileColAtt, quantilePart, confidence, error, seed, scan)).toList(0), logicalPlanToTable, sampleParquetToTable)

    var out = "["
    println(sparkSession.sqlContext.sql(tempQuery).queryExecution.executedPlan)
    val plan = sparkSession.sqlContext.sql(tempQuery).queryExecution.optimizedPlan
    for (p <- plan.output.toList)
      if (p.name == quantileCol)
        quantileColAtt = p.asInstanceOf[AttributeReference]
    val optimizedPhysicalPlans = sparkSession.sessionState.executePlan(Quantile(quantileColAtt, quantilePart, confidence
      , error, seed, plan)).executedPlan
    //optimizedPhysicalPlans.executeCollectPublic().foreach(x => out += (x.mkString(";") + "\n"))
    println(optimizedPhysicalPlans)

    optimizedPhysicalPlans.executeCollectPublic().foreach(x => out += ("{\"percent\":" + x.getDouble(0) + ",\"value\":" + x.getInt(1) + "}," + "\n"))
    out = out.dropRight(2) + "]"
    println(out)
    out
  }

  def execBinning(sparkSession: SparkSession, table: String, binningCol: String, binningPart: Int
                  , binningStart: Double, binningEnd: Double, confidence: Double, error: Double, seed: Long) = {
    //sparkSession.experimental.extraStrategies = Seq(SketchPhysicalTransformation)
    var out = "["
    val scan = sparkSession.sqlContext.sql("select * from " + table).queryExecution.optimizedPlan
    var binningColAtt: AttributeReference = null
    for (p <- scan.output.toList)
      if (p.name == binningCol)
        binningColAtt = p.asInstanceOf[AttributeReference]
    var optimizedPhysicalPlans = sparkSession.sessionState.planner.plan(ReturnAnswer(Binning(binningColAtt, binningPart
      , binningStart, binningEnd, confidence, error, seed, scan))).toList(0)
    //optimizedPhysicalPlans.executeCollectPublic().foreach(x => out += (x.mkString(";") + "\n"))
    executeAndStoreSketch(optimizedPhysicalPlans)
    executeAndStoreSample(sparkSession,optimizedPhysicalPlans)
    optimizedPhysicalPlans=changeSynopsesWithScan(sparkSession,optimizedPhysicalPlans)
    optimizedPhysicalPlans.executeCollectPublic().foreach(x => out += ("{\"start\":" + x.get(0) + ",\"end\":" + x.get(1) + ",\"count\":" + x.get(2) + "}," + "\n"))
    out.dropRight(2) + "]"
  }

  def execDataProfile(sparkSession: SparkSession, dataProfileTable: String, confidence: Double, error: Double
                      , seed: Long) = {
    var out = "["
    var query_code = "select * from " + dataProfileTable
    val scan = sparkSession.sqlContext.sql(query_code).queryExecution.optimizedPlan
    var optimizedPhysicalPlans = sparkSession.sessionState.planner.plan(ReturnAnswer(UniformSampleWithoutCI(seed, scan))).toList(0)
    val logicalPlanToTable: mutable.HashMap[String, String] = new mutable.HashMap()
    recursiveProcess(sparkSession.sqlContext.sql(query_code).queryExecution.analyzed, logicalPlanToTable)
    val sampleParquetToTable: mutable.HashMap[String, String] = new mutable.HashMap()
    //getTableNameToSampleParquet(optimizedPhysicalPlans, logicalPlanToTable, sampleParquetToTable)
    executeAndStoreSketch(optimizedPhysicalPlans)
    for (a <- sampleParquetToTable.toList) {
      if (sparkSession.sqlContext.tableNames().contains(a._1.toLowerCase)) {
        query_code = query_code.replace(a._2.toUpperCase, a._1)
        sparkSession.experimental.extraStrategies = Seq()
        sparkSession.experimental.extraOptimizations = Seq()
        optimizedPhysicalPlans = sparkSession.sqlContext.sql(query_code).queryExecution.executedPlan
      }
    }
    val fraction = 10
    val schema = optimizedPhysicalPlans.schema
    val columns = optimizedPhysicalPlans.schema.toList.map(x => Array[Double](x.dataType match {
      case TimestampType =>
        0
      case StringType =>
        1
      case BooleanType =>
        2
      case DateType =>
        3
      case BinaryType =>
        4
      case DoubleType =>
        5
      case FloatType =>
        6
      case IntegerType =>
        7
      case ByteType =>
        8
      case LongType =>
        9
      case ShortType =>
        10
      case _ =>
        11
    }, 0, 0, 0, 0, 0, 0, 0, 0))
    optimizedPhysicalPlans.executeCollect.foreach(x => for (i <- 0 to columns.size - 1) {
      if (!x.isNullAt(i)) {
        columns(i)(1) += 1
        columns(i)(0) match {
          case 5.0 =>
            val value = x.getDouble(i)
            if (value < columns(i)(3))
              columns(i)(3) = value
            if (value > columns(i)(4))
              columns(i)(4) = value
            columns(i)(5) += value
            columns(i)(6) += value
            columns(i)(7) += value
            columns(i)(8) += value
          case 6.0 =>
            val value = x.getFloat(i)
            if (value < columns(i)(3))
              columns(i)(3) = value
            if (value > columns(i)(4))
              columns(i)(4) = value
            columns(i)(5) += value
            columns(i)(6) += value
            columns(i)(7) += value
            columns(i)(8) += value
          case 7.0 =>
            val value = x.getInt(i)
            if (value < columns(i)(3))
              columns(i)(3) = value
            if (value > columns(i)(4))
              columns(i)(4) = value
            columns(i)(5) += value
            columns(i)(6) += value
            columns(i)(7) += value
            columns(i)(8) += value
          case 9.0 =>
            val value = x.getLong(i)
            if (value < columns(i)(3))
              columns(i)(3) = value
            if (value > columns(i)(4))
              columns(i)(4) = value
            columns(i)(5) += value
            columns(i)(6) += value
            columns(i)(7) += value
            columns(i)(8) += value
          case 10.0 =>
            val value = x.getShort(i)
            if (value < columns(i)(3))
              columns(i)(3) = value
            if (value > columns(i)(4))
              columns(i)(4) = value
            columns(i)(5) += value
            columns(i)(6) += value
            columns(i)(7) += value
            columns(i)(8) += value
          case _ =>

        }
      }
    })
    val s = new Array[String](columns.size)
    for (i <- 0 to columns.size - 1) {
      val ttype = columns(i)(0).toInt match {
        case 0 =>
          "TimeStamp"
        case 1 =>
          "String"
        case 2 =>
          "Boolean"
        case 3 =>
          "Date"
        case 4 =>
          "Binary"
        case 5 =>
          "Double"
        case 6 =>
          "Float"
        case 7 =>
          "Integer"
        case 8 =>
          "Byte"
        case 9 =>
          "Long"
        case 10 =>
          "Short"
        case _ =>
          "Unknown"
      }
      //s(i) = schema(i).name + ";" + ttype + ";" + columns(i).toList(1) * fraction + ";" + columns(i).toList(2) * fraction + ";" + columns(i).toList(3) + ";" + columns(i).toList(4) + ";" + columns(i).toList(5) / columns(i).toList(1) + ";" + columns(i).toList(6) * fraction + ";" + columns(i).toList(7) + ";" + columns(i).toList(8) * fraction
      s(i) = ("{\"name\":\"" + schema(i).name + "\",\"type\":\"" + ttype + "\",\"countNonNull\":"
        + columns(i).toList(1).toInt * fraction + ",\"countDistinct\":" + columns(i).toList(2).toInt * fraction + ",\"min\":"
        + columns(i).toList(3) + ",\"max\":" + columns(i).toList(4) + ",\"avg\":" + columns(i).toList(5) / columns(i).toList(1)
        + ",\"sum\":" + columns(i).toList(6) * fraction + ",\"avgDistinct\":" + columns(i).toList(7).toInt + ",\"sumDistinct\":" + columns(i).toList(8) * fraction) + "},"

    }
    s.foreach(x => out += (x + "\n"))
    out.dropRight(2) + "]"
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

  def changeSynopsesWithScan(sparkSession: SparkSession,plan: SparkPlan): SparkPlan = {
    ruleOfSynopsesToScan(sparkSession).foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
  }

  def ruleOfSynopsesToScan(sparkSession: SparkSession): Seq[Rule[SparkPlan]] = Seq(
    ChangeSampleToScan(sparkSession)
  )


  def executeAndStoreSample(sparkSession: SparkSession,pp: SparkPlan): Unit = {
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

  def folderSize(directory: File): Long = {
    var length: Long = 0
    for (file <- directory.listFiles) {
      if (file.isFile) length += file.length
      else length += folderSize(file)
    }
    length / 100000
  }

  def recursiveProcess(in: LogicalPlan, map: mutable.HashMap[String, String]): Unit = {
    in match {
      case SubqueryAlias(name1, child@SubqueryAlias(name2, lr@LogicalRDD(output, rdd, o, p, f))) =>
        map.put(output.map(_.name).slice(0, 15).mkString(""), name2.identifier)
      case SubqueryAlias(name2, lr@LogicalRDD(output, rdd, o, p, f)) =>
        map.put(output.map(_.name).slice(0, 15).mkString(""), name2.identifier)
      case _ =>
        in.children.map(child => recursiveProcess(child, map))
    }
  }

}