import java.io.{BufferedReader, File, InputStreamReader}
import java.net.ServerSocket
import java.nio.charset.StandardCharsets

import definition.TableDefs
import extraSQLOperators.extraSQLOperators
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.{DataFrame, SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReturnAnswer, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{LogicalRDD, RDDScanExec, SparkPlan}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import rules.logical.ApproximateInjector
import rules.physical.{SampleTransformation, SketchPhysicalTransformation}

import util.control.Breaks._
import scala.collection.{Seq, mutable}
import scala.collection.mutable.ListBuffer
import scala.io.Source
import java.io.FilenameFilter
import java.io.FilenameFilter
import java.util.regex.Pattern

import operators.logical.DistinctSample
import operators.physical.DistinctSampleExec2
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical._

object main {
  val parentDir = "/home/hamid/TASTER/"
  //val parentDir="/home/sdlhshah/spark-data/"
  val pathToSaveSynopses = parentDir + "materializedSynopsis/"
  val pathToSaveSchema = parentDir + "materializedSchema/"

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .appName("Taster")
      .master("local[*]")
      .getOrCreate();
    System.setProperty("geospark.global.charset", "utf8")
    sparkSession.sparkContext.setLogLevel("ERROR");
    sparkSession.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkSession.conf.set("spark.driver.maxResultSize", "8g")
    sparkSession.conf.set("spark.sql.codegen.wholeStage", false); // disable codegen
    val seed = 5427500315423L
    val (bench, format, run, plan, option, repeats, currendDir, parentDir, benchDir, dataDir) = analyzeArgs(args);
    val queries = queryWorkload(bench, benchDir)
    val (extraOpt, extraStr) = setRules(option)
    loadTables(sparkSession, bench, dataDir)

    val server = new ServerSocket(4545)
    println("Serve initialized:")
    //  while (true) {
    for (query <- queries) {
      val folder = (new File(pathToSaveSynopses)).listFiles.filter(_.isDirectory)
      for (i <- 0 to folder.size - 1) {
        if (!folder(i).getName.contains("_parquet") && !folder.find(_.getName == folder(i).getName + "_parquet").isDefined) {
          val f = new File(pathToSaveSynopses + folder(i).getName)
          val filenames = f.listFiles(new FilenameFilter() {
            override def accept(dir: File, name: String): Boolean = name.startsWith("part-")
          })
          var header: Seq[String] = null
          for (line <- Source.fromFile(pathToSaveSchema + folder(i).getName).getLines)
            header = line.split(',').toSeq

          val sqlcontext = sparkSession.sqlContext
          var theUnion: DataFrame = null
          for (file <- filenames) {
            val d = sqlcontext.read.format("com.databricks.spark.csv").option("delimiter", ",").load(file.getPath)
            if (theUnion == null) theUnion = d
            else theUnion = theUnion.union(d)
          }
          theUnion.toDF(header: _*).write.format("parquet").save(pathToSaveSynopses + folder(i).getName + "_parquet");
        }
      }
      updateSynopsesView(sparkSession, pathToSaveSynopses)
      //  val clientSocket = server.accept()
      // val input = clientSocket.getInputStream()
      //  val output = clientSocket.getOutputStream()
      // try {
      //   var query = java.net.URLDecoder.decode(new BufferedReader(new InputStreamReader(input)).readLine, StandardCharsets.UTF_8.name)
      //   breakable {
      //      if (!query.contains("GET /QAL?query=")) {
      //    outString = "{'status':400,'message':\"Wrong REST request!!!\"}"
      //    break()
      //  }
      //   }
      //  query = query.replace("GET /QAL?query=", "").replace(" HTTP/1.1", "")
      var (query_code, confidence, error, dataProfileTable, quantileCol, quantilePart, binningCol, binningPart
      , binningStart, binningEnd, table, tempQuery) = tokenizeQuery(query)
      sparkSession.experimental.extraOptimizations = Seq(new ApproximateInjector(confidence, error, seed));
      sparkSession.experimental.extraStrategies = extraStr;
      println("Query:")
      println(query_code)
      val time = System.nanoTime()
      var outString = ""
      if (quantileCol != "") {
        outString = extraSQLOperators.execQuantile(sparkSession, tempQuery, table, quantileCol, quantilePart, confidence, error, seed)
      } else if (binningCol != "")
        outString = extraSQLOperators.execBinning(sparkSession, table, binningCol, binningPart, binningStart, binningEnd, confidence, error, seed)
      else if (dataProfileTable != "")
        outString = extraSQLOperators.execDataProfile(sparkSession, dataProfileTable, confidence, error, seed)
      else if (plan) {
        val logicalPlanToTable: mutable.HashMap[String, String] = new mutable.HashMap()
        recursiveProcess(sparkSession.sqlContext.sql(query_code).queryExecution.analyzed, logicalPlanToTable)
        val sampleParquetToTable: mutable.HashMap[String, String] = new mutable.HashMap()
        getTableNameToSampleParquet(sparkSession.sqlContext.sql(query_code).queryExecution.executedPlan, logicalPlanToTable, sampleParquetToTable)
        for (a <- sampleParquetToTable.toList) {
          if (sparkSession.sqlContext.tableNames().contains(a._1.toLowerCase))
            query_code = query_code.replace(a._2.toUpperCase, a._1)
        }

        //   sampleParquetToTable.foreach()
        query_code

     //   sparkSession.experimental.extraStrategies = Seq()
    //    sparkSession.experimental.extraOptimizations = Seq()
        var time=System.nanoTime()
        sparkSession.sqlContext.sql(query_code).show(20000)
        println((System.nanoTime()-time)/1000000000)

        throw new Exception("Done")
        //  println(sparkSession.sqlContext.sql(query_code).queryExecution.executedPlan)
        val col = sparkSession.sqlContext.sql(query_code).columns
        val minNumberOfOcc = 15
        val partCNT = 15
        var fraction = (18 / 4)

        //    if(query_code.contains(" group by s.province"))
        //      fraction=
        //     time=System.nanoTime()
        //  sparkSession.sqlContext.sql(query_code).show(20000)
        // println(sparkSession.sqlContext.sql(query_code).queryExecution.executedPlan)
        //  println("Approximate execution time with generating sample:"+(System.nanoTime()-time)/100000)
        //   time=System.nanoTime()
        // println(sparkSession.sqlContext.sql(query_code).collect.map(x=>x.getLong(0)).reduce(_+_))
        // println("next")
        //    println(sparkSession.sqlContext.sql(query_code).collect.map(x=>x.getLong(0)).reduce(_+_))
        println(sparkSession.sqlContext.sql(query_code).queryExecution.executedPlan)
        println(sparkSession.sqlContext.sql(query_code).queryExecution.executedPlan)
        val plan = sparkSession.sqlContext.sql(query_code).queryExecution.executedPlan
        val out = sparkSession.sqlContext.sql(query_code).collect //.map(x=>x.mkString(",")).foreach(z=>println(z))
        outString = "[" + out.map(row => {
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
        }).toArray.mkString(",\n") + "]"
        // println(outString)
        /*println("Approximate execution time with prepared sample:"+(System.nanoTime()-time)/100000)
        val rawPlan = sparkSession.sqlContext.sql(query_code).queryExecution.analyzed
        val optimizedLogicalPlan = sparkSession.sqlContext.sql(query_code).queryExecution.optimizedPlan
        //val optimizedPhysicalPlans = sparkSession.sessionState.executePlan(optimizedLogicalPlan).executedPlan
        //  optimizedPhysicalPlans.executeCollect().foreach(x => println( x.getInt(0)))


        println(sparkSession.sessionState.planner.plan(ReturnAnswer(sparkSession.sharedState.cacheManager.useCachedData(sparkSession.sessionState.optimizer.execute(optimizedLogicalPlan)))).toList(0))
        val x = sparkSession.sessionState.planner.plan(ReturnAnswer(sparkSession.sharedState.cacheManager.useCachedData(sparkSession.sessionState.optimizer.execute(optimizedLogicalPlan)))).toList(0)
        println(x.executeCollectPublic().toList.size)
        // val optimizedPhysicalPlans = sparkSession.sessionState.planner.plan(ReturnAnswer(optimizedLogicalPlan)).toList(0)
        val optimizedPhysicalPlans = sparkSession.sessionState.executePlan(optimizedLogicalPlan).executedPlan
        println("Raw Plans:")
        println(rawPlan)
        println("Optimized Approximate Plan:")
        println(optimizedLogicalPlan)
        println("Optimized Physical Plans:")
        println(optimizedPhysicalPlans)
        //     for (optimizedPhysicalPlans <- sparkSession.sessionState.planner.plan(ReturnAnswer(optimizedLogicalPlan)).toList) {
        //     println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
        //            println(optimizedPhysicalPlans.children(0).children(0).children(0).executeCollectPublic().size)
        //          println(optimizedPhysicalPlans.children(0).children(0).children(0).executeCollectPublic().size)

        optimizedPhysicalPlans.executeCollectPublic().foreach(x => outString += (x + "\n"))
        println(optimizedPhysicalPlans.executeCollectPublic().size)
        println(outString)*/
        //}
        //   val responseDocument = (outString).getBytes("UTF-8")

        //    val responseHeader = ("HTTP/1.1 200 OK\r\n" + "Content-Type: text/html; charset=UTF-8\r\n" + "Content-Length: " + responseDocument.length + "\r\n\r\n").getBytes("UTF-8")


      }
      println("Execution time: " + (System.nanoTime() - time) / 100000000)

      //  val responseDocument = (outString).getBytes("UTF-8")

      //  val responseHeader = ("HTTP/1.1 200 OK\r\n" + "Content-Type: text/html; charset=UTF-8\r\n" + "Content-Length: " + responseDocument.length + "\r\n\r\n").getBytes("UTF-8")

      //   output.write(responseHeader)
      //  output.write(responseDocument)
      //   input.close()
      //  output.close()

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

  def getTableNameToSampleParquet(in: SparkPlan, logicalToTable: mutable.HashMap[String, String], map: mutable.HashMap[String, String]): Unit = {
    in match {
      case sample@DistinctSampleExec2(functions, confidence, error, seed, groupingExpression, child@RDDScanExec(output, rdd, outputPartitioning, outputOrderingSeq, isStreaming)) =>
        map.put(sample.toString() + "_parquet", logicalToTable.getOrElse(output.map(_.name).slice(0, 15).mkString(""), "Missing logical plan to table!!"))
      case _ =>
        in.children.map(child => getTableNameToSampleParquet(child, logicalToTable, map))
    }
  }

  def recursiveProcess(in: LogicalPlan, map: mutable.HashMap[String, String]): Unit = {
    in match {
      case SubqueryAlias(name1, child@SubqueryAlias(name2, lr@LogicalRDD(output, rdd, o, p, f))) =>
        map.put(output.map(_.name).slice(0, 15).mkString(""), name2.identifier)
      case _ =>
        in.children.map(child => recursiveProcess(child, map))
    }
  }

  def updateSynopsesView(sparkSession: SparkSession, pathToSynopsesFolder: String): Unit = {
    val existingView = sparkSession.sqlContext.tableNames()
    val folder = (new File(pathToSynopsesFolder)).listFiles.filter(_.isDirectory)
    for (i <- 0 to folder.size - 1) {
      if (folder(i).getName.contains("_parquet") && !existingView.contains(folder(i).getName)) {
        val view = sparkSession.read.parquet(pathToSynopsesFolder + folder(i).getName);
        sparkSession.sqlContext.createDataFrame(view.rdd, view.schema).createOrReplaceTempView("Distinct_acheneIDlatlonprovinceisActiveactivityStatusdateOfActivityStartnumberOfEmployeesrevenueEBITDAbalanceSheetClosingDateflagsATECOkeywordstaxID_09_01_01_count1_legalStatus92_parquet" /*folder(i).getName*/);
      }
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

  def queryWorkload(bench: String, BENCH_DIR: String): List[String] = {
    var temp: ListBuffer[String] = ListBuffer();
    if (bench.equals("skyServer") || bench.equals("atoka") || bench.equals("test") || bench.equals("tpch")) {
      val src = Source.fromFile(BENCH_DIR + "queryLog.csv").getLines
      src.take(1).next
      for (l <- src)
        temp.+=(l.split(';')(0))
      temp.toList
    }
    else
      throw new Exception("Invalid input data benchmark")
  }

  def setRules(option: String): (Seq[Rule[LogicalPlan]], Seq[Strategy]) = {
    if (option.equals("precise") || option.contains("offline"))
      (Seq(), Seq())
    else if (option.equals("taster"))
      (Seq(), Seq(/*SketchPhysicalTransformation,*/ SampleTransformation))
    else
      throw new Exception("The engine is not defined")
  }

  def analyzeArgs(args: Array[String]): (String, String, Boolean, Boolean, String, Int, String, String, String, String) = {
    val inputDataBenchmark = args(0);
    val sf = args(1)
    val hdfsOrLocal = args(2)
    val runsetup = args(3)
    val engine = args(4);
    val repeats: Int = args(5).toInt;
    val inputDataFormat = args(6);
    var plan = false;
    var run = false;
    if (runsetup.equals("plan-run")) {
      run = true;
      plan = true;
    } else if (runsetup.equals("plan")) {
      plan = true;
    } else if (runsetup.equals("run")) {
      run = true;
    }
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
    (inputDataBenchmark, inputDataFormat, run, plan, engine, repeats, CURRENTSTATE_DIR, PARENT_DIR, BENCH_DIR
      , PARENT_DIR + "data_" + inputDataFormat + "/")
  }
}



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