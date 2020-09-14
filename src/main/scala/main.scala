import java.io.{BufferedReader, File, InputStreamReader}
import java.net.ServerSocket
import java.nio.charset.StandardCharsets

import definition.TableDefs
import extraSQLOperators.extraSQLOperators
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReturnAnswer, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.RDDScanExec
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import rules.logical.ApproximateInjector
import rules.physical.{SampleTransformation, SketchPhysicalTransformation}

import util.control.Breaks._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
object main {
  val path = "/home/hamid/TASTER/materializedSynopsis/"
  // val path = "/home/sdlhshah/spark-data/materializedSynopsis/"
  val schemaFolderPath = "/home/hamid/TASTER/schemas/"

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
    import sparkSession.implicits._

   //   val sampleParquet = sparkSession.read.parquet( "/home/hamid/Distinct;acheneID|lat|lon|province|isActive|activityStatus|dateOfActivityStart|numberOfEmployees|revenue|EBITDA;0.9;0.1;5427500315423;0;0.05;count(1);legalStatus#92;parquet");
   //   sparkSession.sqlContext.createDataFrame(sampleParquet.rdd, sampleParquet.schema).createOrReplaceTempView("sampleSCV");
  //    sparkSession.sqlContext.sql("select count(*),legalStatus from sampleSCV group by legalStatus").show(2000)
  //    sparkSession.sqlContext.sql("select count(*),s.legalStatus from PFV p, sampleSCV s where p.company_acheneID=s.acheneID and s.revenue>10000 group by s.legalStatus confidence 90 error 10").show(2000)
    //if we have sample

    val seed = 5427500315423L
    val (bench, format, run, plan, option, repeats, currendDir, parentDir, benchDir, dataDir) = analyzeArgs(args);
    val queries = queryWorkload(bench, benchDir)
    val (extraOpt, extraStr) = setRules(option)
    loadTables(sparkSession, bench, dataDir)
    //sparkSession.sqlContext.sql("select count(numberOfEmployees),numberOfEmployees from PFV p, SCV s where p.company_acheneID= s.acheneID  group by numberOfEmployees").show()

  //    sparkSession.sqlContext.sql("select count(*),legalStatus from  sampleSCV s  group by s.legalStatus").show(20000)
  //    sparkSession.sqlContext.sql("select count(*),legalStatus from  SCV s  group by s.legalStatus").show(20000)

    //println(sparkSession.sqlContext.sql("select revenue from SCV s, PFV p where p.company_acheneID=s.acheneID").queryExecution.executedPlan)
    // sparkSession.sqlContext.sql("select avg(p.totalAmount),s.legalStatus from PFV p, SCV s where p.company_acheneID=s.acheneID group by s.legalStatus").show(2000)
    //sparkSession.sqlContext.sql("select totalAmount from PFV p, SCV s where p.company_acheneID=s.acheneID ").queryExecution.optimizedPlan
    val server = new ServerSocket(4545)
    println("Serve initialized:")
    //  while (true) {
    for (query <- queries) {
      val folder = (new File(path)).listFiles.filter(_.isDirectory)
      for (i <- 0 to folder.size - 1) {
        if (!folder(i).getName.contains(".parquet") && !folder.find(_.getName == folder(i).getName + ".parquet").isDefined) {
/*        val filename = schemaFolderPath + folder(i).getName
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
          val view = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "false")
            .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(path + folder(i).getName)
          view.toDF().groupBy("_c22").count().show(10000)
          val newColumns = Seq("acheneID","lat","lon" ,"province","isActive","activityStatus","dateOfActivityStart","numberOfEmployees","revenue","EBITDA","balanceSheetClosingDate","flags","ATECO","keywords","taxID","vatID","numberOfBuildings","numberOfPlotsOfLand","numberOfRealEstates","categoryScore","score","updateTime","legalStatus")
          val df = view.toDF(newColumns:_*)
       //   view.columns.foreach(println)
          view.take(10000).foreach(println)
          println(view.count())
          df.write.format("parquet").save(path + folder(i).getName + ";parquet");
        }
      }
      //  val clientSocket = server.accept()
      // val input = clientSocket.getInputStream()
      //  val output = clientSocket.getOutputStream()
      // try {
      //   var query = java.net.URLDecoder.decode(new BufferedReader(new InputStreamReader(input)).readLine, StandardCharsets.UTF_8.name)
      var outString = ""
      //   breakable {
      //      if (!query.contains("GET /QAL?query=")) {
      //    outString = "{'status':400,'message':\"Wrong REST request!!!\"}"
      //    break()
      //  }
      //   }
      //  query = query.replace("GET /QAL?query=", "").replace(" HTTP/1.1", "")
      val (query_code, confidence, error, dataProfileTable, quantileCol, quantilePart, binningCol, binningPart
      , binningStart, binningEnd, table, tempQuery) = tokenizeQuery(query)
      // sparkSession.sqlContext.sql("select count(*),legalStatus from SCV group by legalStatus").show(2000)
      //sparkSession.sqlContext.sql(query_code).collect().map(x=>x.mkString(",")).foreach(x=>println(x))
      // println("Exact execution time:"+(System.nanoTime()-time)/100000)
      // println(sparkSession.sqlContext.sql(query_code).queryExecution.executedPlan)
      sparkSession.experimental.extraOptimizations = Seq(new ApproximateInjector(confidence, error, seed));
      sparkSession.experimental.extraStrategies = extraStr;
      println("Query:")
      println(query_code)
      val time = System.nanoTime()
      if (quantileCol != "") {
        outString = extraSQLOperators.execQuantile(sparkSession, tempQuery, table, quantileCol, quantilePart, confidence, error, seed)
      } else if (binningCol != "")
        outString = extraSQLOperators.execBinning(sparkSession, table, binningCol, binningPart, binningStart, binningEnd, confidence, error, seed)
      else if (dataProfileTable != "")
        outString = extraSQLOperators.execDataProfile(sparkSession, dataProfileTable, confidence, error, seed)
      else if (plan) {
        //   sparkSession.sqlContext.sql("select sum(revenue/10) from SCV ").show()
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
        val plan=sparkSession.sqlContext.sql(query_code).queryExecution.executedPlan
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
    var tempQuery= ""
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
        tempQuery="select "+quantileCol+ " "+tokens.slice(tokens.indexOf("from"),tokens.indexOf("confidence")).mkString(" ")
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
      , binningStart, binningEnd, table,tempQuery)
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
      (Seq(),Seq(/*SketchPhysicalTransformation,*/ SampleTransformation))
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
