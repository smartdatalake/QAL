package mains

import java.io._
import java.net.ServerSocket
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import costModel.TasterCostModel
import definition.Paths.{ParquetNameToSynopses, SynopsesToParquetName, lastUsedOfParquetSample, mapRDDScanRowCNT, parentDir, parquetNameToHeader, readRDDScanRowCNT, seed, tableName, timeForSampleConstruction, timeForSubQueryExecution, timeForUpdateWarehouse, timeTotal, updateAttributeName, warehouseParquetNameToSize, windowSize, _}
import extraSQL.extraSQLOperators
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BinaryExpression, BinaryOperator, Expression, Literal, Or, SortOrder}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{LogicalRDD, SparkPlan}
import org.apache.spark.sql.types.BooleanType
import rules.logical.{ApproximateInjector, pushFilterUp}
import rules.physical.{SampleTransformation, SketchPhysicalTransformation}

import scala.collection.mutable.ListBuffer
import scala.collection.{Seq, mutable}
import scala.io.Source
import scala.reflect.io.Directory
import scala.util.control.Breaks.breakable

object SDL extends QueryEngine_Abs("QAL_SDL", true) {

  val tableCounter = new mutable.HashMap[String, Int]()
  var Proteus_URL = ""
  var Proteus_username = ""
  var Proteus_pass = ""
  var REST_PORT = 4545
  var REST = true
  val costModel = new TasterCostModel(sparkSession,justAPP,false)

  def main(args: Array[String]): Unit = {
    readConfiguration(args)
    loadTables(sparkSession)
    val x = new mutable.HashMap[String, (Double, Double)]
    var source = Source.fromFile("/home/hamid/exact")
    for (line <- source.getLines())
      x.put(line.split(",")(1).dropRight(1), (line.split(",")(0).drop(1).toDouble, -1))
    source.close()
    source = Source.fromFile("/home/hamid/approx")
    for (line <- source.getLines()) {
      val key = line.split(",")(1).dropRight(1).toString
      val app = line.split(",")(0).drop(1).toDouble*(100.0/50.0)
      val exact = x.get(key).get
      x.put(key, (exact._1, math.abs((exact._1 - app) / exact._1)))
    }
    x.map(x=>x._2._2).toList.sortBy(_.toString).foreach(println)
    source.close()
    source = Source.fromFile("/home/hamid/hh")
    var a=new ListBuffer[Double]()
    var t=0.0
    for (line <- source.getLines()) {
      a.+=(line.toDouble)
      t+=line.toDouble
    }
    a=a.sortBy(_.toString)
a.foreach(println)
    println(t/a.size.toDouble)
 //   throw new Exception

   //sparkSession.sql("select count(*),province from pft p, sct s where p.company_acheneid=s.acheneid group by s.province")/*.collect().map(x=>"["+x.get(0).toString+","+x.get(1).toString+"]").foreach(println)*/.show(1000)
    //sparkSession.sql("select count(*),numberOfEmployees from sct group by numberOfEmployees").show(10000)
    mapRDDScanRowCNT = readRDDScanRowCNT(sparkSession)
    //sparkSession.experimental.extraStrategies = Seq(SampleTransformationMultiple)
    sparkSession.experimental.extraOptimizations = Seq(new ApproximateInjector(confidence, error, seed), new pushFilterUp())
    sparkSession.experimental.extraStrategies = Seq(/*extraRulesWithoutSampling,*/ SketchPhysicalTransformation, SampleTransformation);

    if (REST) {
      val queryLog = new ListBuffer[String]()
      val server = new ServerSocket(REST_PORT)
      println("Server initialized:")
      while (true) {
        val clientSocket = server.accept()
        val input = clientSocket.getInputStream()
        val output = clientSocket.getOutputStream()
        val inputHTTP = java.net.URLDecoder.decode(new BufferedReader(new InputStreamReader(input)).readLine, StandardCharsets.UTF_8.name).toLowerCase
        var out = ""
        var responseDocument: Array[Byte] = null
        var responseHeader: Array[Byte] = null
        val t = System.nanoTime()
        breakable {
          if (inputHTTP.contains("get /flushsample")) { // GET remove PROTEUS
            flush1()
            out = "{'status':200,'message':\"Warehouse is flushed.\"}"
            responseDocument = (out).getBytes("UTF-8")
            responseHeader = ("HTTP/1.1 404 FAIL\r\n" + "Content-Type: text/html; charset=UTF-8\r\n" + "Content-Length: " + responseDocument.length + "\r\n\r\n").getBytes("UTF-8")
          }
          else if (inputHTTP.contains("get /removetable")) { // GET remove PROTEUS
            flush2()
            out = "{'status':200,'message':\"Stored tables are removed.\"}"
            responseDocument = (out).getBytes("UTF-8")
            responseHeader = ("HTTP/1.1 404 FAIL\r\n" + "Content-Type: text/html; charset=UTF-8\r\n" + "Content-Length: " + responseDocument.length + "\r\n\r\n").getBytes("UTF-8")
          }
          else if (inputHTTP.contains("get /changewrsize")) {
            if (inputHTTP.contains("quota")) {
              val params = inputHTTP.split('?')(1).replace(" http/1.1", "").split('&').map(_.split('='))
              maxSpace = Integer.parseInt(params.find(x => x(0).contains("quota")).get(1)) * 10
              out = "{'status':200,'message':\"Warehouse quota is updated.\"}"
              responseDocument = (out).getBytes("UTF-8")
              responseHeader = ("HTTP/1.1 404 FAIL\r\n" + "Content-Type: text/html; charset=UTF-8\r\n" + "Content-Length: " + responseDocument.length + "\r\n\r\n").getBytes("UTF-8")
            }
            else {
              out = "{'status':404,'message':\"Missing parameter quota, update canceled.\"}"
              responseDocument = (out).getBytes("UTF-8")
              responseHeader = ("HTTP/1.1 404 FAIL\r\n" + "Content-Type: text/html; charset=UTF-8\r\n" + "Content-Length: " + responseDocument.length + "\r\n\r\n").getBytes("UTF-8")
            }
          }
          else if (inputHTTP.contains("get /alive")) { // Alive
            out = "{'status':200,'message':\"I am. Space quota:" + maxSpace / 10 + "mb, " + warehouseParquetNameToSize + "" + SynopsesToParquetName + "\"}"
            responseDocument = (out).getBytes("UTF-8")
            responseHeader = ("HTTP/1.1 404 FAIL\r\n" + "Content-Type: text/html; charset=UTF-8\r\n" + "Content-Length: " + responseDocument.length + "\r\n\r\n").getBytes("UTF-8")
          }
          else if (inputHTTP.contains("get /changeproteus")) { // GET change PROTEUS
            if (inputHTTP.contains("url") && inputHTTP.contains("username") && inputHTTP.contains("pass")) {
              val params = inputHTTP.split('?')(1).replace(" http/1.1", "").split('&').map(_.split('='))
              Proteus_URL = params.find(x => x(0).contains("url")).get(1)
              Proteus_username = params.find(x => x(0).contains("username")).get(1)
              Proteus_pass = params.find(x => x(0).contains("pass")).get(1)
              out = "{'status':200,'message':\"Proteus credential is changed.\"}"
              responseDocument = (out).getBytes("UTF-8")
              responseHeader = ("HTTP/1.1 404 FAIL\r\n" + "Content-Type: text/html; charset=UTF-8\r\n" + "Content-Length: " + responseDocument.length + "\r\n\r\n").getBytes("UTF-8")
            }
            else {
              out = "{'status':404,'message':\"Missing fields, update skipped.\"}"
              responseDocument = (out).getBytes("UTF-8")
              responseHeader = ("HTTP/1.1 404 FAIL\r\n" + "Content-Type: text/html; charset=UTF-8\r\n" + "Content-Length: " + responseDocument.length + "\r\n\r\n").getBytes("UTF-8")
            }

          }
          else if (inputHTTP.contains("get /qal?query=")) { // GET execute a query
            val query = inputHTTP.replace("get /qal?query=", "").replace(" http/1.1", "")
            //     try {
            if (query.contains("confidence") && query.contains("error")) queryLog += (query)
            val past = if (queryLog.filter(x => (!x.contains("binning") && !x.contains("quantile") && !x.contains("dataprofile"))).size >= windowSize) queryLog.filter(x => (!x.contains("binning") && !x.contains("quantile") && !x.contains("dataprofile"))).slice(queryLog.size - windowSize, queryLog.size) else queryLog.filter(x => (!x.contains("binning") && !x.contains("quantile") && !x.contains("dataprofile"))).slice(0, queryLog.size)
            out = executeQuery(query, past.toList)
            responseDocument = (out).getBytes("UTF-8")
            responseHeader = ("HTTP/1.1 200 OK\r\n" + "Content-Type: text/html; charset=UTF-8\r\n" + "Content-Length: " + responseDocument.length + "\r\n\r\n").getBytes("UTF-8")
            //        }
            /*          catch {
                          case e: Exception =>
                         println("I faced error")
                            println(e.getMessage)
                               responseDocument = ("{'status':404,'message':\"" + e.getMessage + "\"}").getBytes("UTF-8")
                             responseHeader = ("HTTP/1.1 404 FAIL\r\n" + "Content-Type: text/html; charset=UTF-8\r\n" + "Content-Length: " + responseDocument.length + "\r\n\r\n").getBytes("UTF-8")
                         }*/
          }
          else { // GET invalid REST request
            out = "{'status':404,'message':\"Invalid REST request!!!\"}"
            responseDocument = (out).getBytes("UTF-8")
            responseHeader = ("HTTP/1.1 404 FAIL\r\n" + "Content-Type: text/html; charset=UTF-8\r\n" + "Content-Length: " + responseDocument.length + "\r\n\r\n").getBytes("UTF-8")

          }
          println((System.nanoTime() - t) / 1000000000)
          output.write(responseHeader)
          output.write(responseDocument)
          input.close()
          output.close()
        }
      }
    } // No REST API
    else {
      val queries = loadWorkload("atoka", sparkSession)
      for (i <- 0 to queries.size - 1) {
        val out = executeQuery(queries(i), queries.slice(i, i + windowSize))
        println(out)
      }
    }
  }

  def executeQuery(query: String, future: List[String]): String = {
    println(query)
    timeTotal = System.nanoTime()
    var outString = ""
    val (query_code, confidence, error, dataProfileTable, quantileCol, quantilePart, binningCol, binningPart
    , binningStart, binningEnd, table, tempQuery) = tokenizeQuery(query)
    sparkSession.experimental.extraOptimizations = Seq(new ApproximateInjector(confidence, error, seed), new pushFilterUp)
    sparkSession.experimental.extraStrategies = Seq(/*extraRulesWithoutSampling,*/ SketchPhysicalTransformation, SampleTransformation);
    //  try {

    if (confidence == 0) {
      val x = sparkSession.experimental.extraOptimizations
      val xx = sparkSession.experimental.extraStrategies
      sparkSession.experimental.extraOptimizations = Seq()
      sparkSession.experimental.extraStrategies = Seq()
      try {
        val p = sparkSession.sessionState.planner.plan(ReturnAnswer(sparkSession.sessionState.optimizer.execute(sparkSession.sqlContext.sql(query).queryExecution.analyzed))).toList(0)
        val cheapestPhysicalPlan = prepareForExecution(p, sparkSession)
        println(cheapestPhysicalPlan)
        //////////////////////////////////////////////////////////////////////////////////////////////////////////

        //execute the best approximate physical plan with generated synopses
        //////////////////////////////////////////////////////////////////////////////////////////////////////////
        cheapestPhysicalPlan.executeCollectPublic().toList.foreach(row => {
          outString += row.toString() + "\n"
        })
        sparkSession.experimental.extraOptimizations = x
        sparkSession.experimental.extraStrategies = xx
      }
      catch {
        case e: Throwable =>
          sparkSession.experimental.extraOptimizations = x
          sparkSession.experimental.extraStrategies = xx
          return e.toString
      }
    }
    else if (quantileCol != "") {
      sparkSession.experimental.extraStrategies = Seq(SketchPhysicalTransformation, SampleTransformation);
      val logicalPlan = sparkSession.sessionState.sqlParser.parsePlan(tempQuery)
      checkAndCreateTable(logicalPlan)
      outString = extraSQLOperators.execQuantile(sparkSession, tempQuery, table, quantileCol, quantilePart, confidence, error, seed)
    } else if (binningCol != "") {
      sparkSession.experimental.extraStrategies = Seq(SketchPhysicalTransformation, SampleTransformation);
      val logicalPlan = sparkSession.sessionState.sqlParser.parsePlan(tempQuery)
      checkAndCreateTable(logicalPlan)
      outString = extraSQLOperators.execBinning(sparkSession, table, binningCol, binningPart, binningStart, binningEnd, confidence, error, seed, tempQuery)
    } else if (dataProfileTable != "") {
      val logicalPlan = sparkSession.sessionState.sqlParser.parsePlan("select * from " + dataProfileTable)
      checkAndCreateTable(logicalPlan)
      sparkSession.experimental.extraStrategies = Seq(SketchPhysicalTransformation, SampleTransformation);
      outString = extraSQLOperators.execDataProfile(sparkSession, dataProfileTable, confidence, error, seed)
    } else {
      checkAndCreateTable(sparkSession.sessionState.sqlParser.parsePlan(query_code))
      sparkSession.experimental.extraStrategies = Seq(/*SketchPhysicalTransformation,*/ SampleTransformation);
      costModel.addQuery(query_code, "", 0, null)
      //appPhysicalPlan=
      // val subQuery = sparkSession.sqlContext.sql(query_code).queryExecution.analyzed
      // updateAttributeName(subQuery, new mutable.HashMap[String, Int]())
      // val joins = enumerateRawPlanWithJoin(subQuery)
      //val logicalPlans = joins.map(x => sparkSession.sessionState.optimizer.execute(x))
      // val pps = logicalPlans.flatMap(x => sparkSession.sessionState.planner.plan(ReturnAnswer(x)))

      //choose the best approximate physical plan and create related synopses, presently, the lowest-cost plan
      //////////////////////////////////////////////////////////////////////////////////////////////////////////
      val checkpointForSampleConstruction = System.nanoTime()

      // println(logicalPlans.flatMap(x => sparkSession.sessionState.planner.plan(ReturnAnswer(x)))).map(x => (x, ))
      val pp = costModel.suggest().toList(0)
      //logicalPlans.flatMap(x => sparkSession.sessionState.planner.plan(ReturnAnswer(x))).toList(0)
      //val pp = sparkSession.sessionState.planner.plan(ReturnAnswer(sparkSession.sessionState.optimizer.execute(analyzed))).toList(0)
      var cheapestPhysicalPlan = changeSynopsesWithScan(pp)
      executeAndStoreSample(cheapestPhysicalPlan, sparkSession)
      cheapestPhysicalPlan = changeSynopsesWithScan(cheapestPhysicalPlan)
      cheapestPhysicalPlan = prepareForExecution(cheapestPhysicalPlan, sparkSession)
      executeAndStoreSketch(cheapestPhysicalPlan)
      println(cheapestPhysicalPlan)
      timeForSampleConstruction += (System.nanoTime() - checkpointForSampleConstruction)
      //////////////////////////////////////////////////////////////////////////////////////////////////////////

      //execute the best approximate physical plan with generated synopses
      //////////////////////////////////////////////////////////////////////////////////////////////////////////
      val checkpointForSubQueryExecution = System.nanoTime()
      countReusedSample(cheapestPhysicalPlan)
      cheapestPhysicalPlan.executeCollectPublic().toList.foreach(row => {
        outString += row.toString() + "\n"
      })
      timeForSubQueryExecution += (System.nanoTime() - checkpointForSubQueryExecution)
    }
    // }

    // catch {
    //   case e: Throwable => return e.toString
    // }
    updateWarehouse(future)

    numberOfExecutedSubQuery += 1
    //////////////////////////////////////////////////////////////////////////////////////////////////////////

    //update warehouse, LRU or window based
    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    val checkpointForWarehouseUpdate = System.nanoTime()
    timeForUpdateWarehouse += (System.nanoTime() - checkpointForWarehouseUpdate)
    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    var oos: ObjectOutputStream = new ObjectOutputStream(new FileOutputStream(parentDir + "warehouseParquetNameToSize.ser"))
    oos.writeObject(warehouseParquetNameToSize)
    oos.close()
    oos = new ObjectOutputStream(new FileOutputStream(parentDir + "ParquetNameToSynopses.ser"))
    oos.writeObject(ParquetNameToSynopses)
    oos.close()
    oos = new ObjectOutputStream(new FileOutputStream(parentDir + "SynopsesToParquetName.ser"))
    oos.writeObject(SynopsesToParquetName)
    oos.close()
    oos = new ObjectOutputStream(new FileOutputStream(parentDir + "lastUsedOfParquetSample.ser"))
    oos.writeObject(lastUsedOfParquetSample)
    oos.close()
    oos = new ObjectOutputStream(new FileOutputStream(parentDir + "parquetNameToHeader.ser"))
    oos.writeObject(parquetNameToHeader)
    oos.close()
    oos = new ObjectOutputStream(new FileOutputStream(parentDir + "sampleToOutput.ser"))
    oos.writeObject(sampleToOutput)
    oos.close()
    tableName.clear()
    outString
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


  def checkAndCreateTable(lp: LogicalPlan): Unit = lp match {
    case org.apache.spark.sql.catalyst.analysis.UnresolvedRelation(table) =>
      val listOfFiles = new File(pathToTableParquet).listFiles
      var getTable = true
      for (j <- 0 until listOfFiles.length)
        if (listOfFiles(j).getName == table.table.toLowerCase + ".parquet") getTable = false
      if (getTable)
        getAndCreateTableFromProteus(table.table.toLowerCase)
    case t =>
      t.children.foreach(child => checkAndCreateTable(child))
  }

  def flush1(): Unit = {
    (new File(pathToSaveSynopses)).listFiles.filter(_.getName.contains(".obj")).foreach(Directory(_).deleteRecursively())
    (new File(pathToSketches)).listFiles.foreach(x => x.delete())

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

  def flush2(): Unit = {
    (new File(pathToTableCSV)).listFiles.foreach(x => x.delete())
    (new File(pathToTableParquet)).listFiles.foreach(Directory(_).deleteRecursively())

  }

  def getAndCreateTableFromProteus(tableName: String): Unit = {
    System.out.println("fetching table from proteus " + tableName)
    Class.forName("org.apache.calcite.avatica.remote.Driver")
    val connection: Connection = DriverManager.getConnection(Proteus_URL, Proteus_username, Proteus_pass)
    val statement = connection.createStatement()
    // println("select * from " + tableName + " limit " + JDBCRowLimiter)
    val rs = statement.executeQuery("select * from " + tableName)
    val rsmd = rs.getMetaData

    var header = ""
    for (i <- 1 to rsmd.getColumnCount) {
      header += rsmd.getColumnName(i) + ","
    }
    header = header.substring(0, header.length - 1)
    val numberOfColumns = rsmd.getColumnCount
    val writer = new BufferedWriter(new FileWriter(pathToTableCSV + "/" + tableName + ".csv"))
    writer.write(header + "\n")
    while ( {
      rs.next
    }) {
      var row = ""
      for (i <- 1 to numberOfColumns) {
        val value = rs.getString(i)
        if (rs.getObject(i) != null) row += value.replace(',', ' ')
        if (i < numberOfColumns) row += ','
      }
      writer.write(row + "\n")
    }
    writer.close()
    System.out.println("fetched table from proteus " + tableName)


    val view = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(pathToTableCSV + "/" + tableName + ".csv")
    sparkSession.sqlContext.createDataFrame(view.rdd, view.schema).createOrReplaceTempView(tableName.toLowerCase);
    view.write.format("parquet").save(pathToTableParquet + "/" + tableName.toLowerCase + ".parquet");
    val lRDD = sparkSession.sessionState.catalog.lookupRelation(org.apache.spark.sql.catalyst.TableIdentifier
    (tableName, None)).children(0).asInstanceOf[LogicalRDD]
    // new File(pathToTableCSV + "/" + tableName + ".csv").delete()
    mapRDDScanRowCNT.put(lRDD.output.map(o => tableName + "." + o.name.split("#")(0)).mkString(";").toLowerCase, folderSize(new File(pathToTableParquet + "/" + tableName.toLowerCase + ".parquet")))
  }

  override def readConfiguration(args: Array[String]): Unit = {
    val json = Source.fromFile("QALconf.txt")
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val parsedJson = mapper.readValue[Map[String, String]](json.reader())
    maxSpace = parsedJson.getOrElse("maxSpace", "1000").toInt
    windowSize = parsedJson.getOrElse("windowSize", "5").toInt
    REST = parsedJson.getOrElse("REST", "true").toBoolean
    parentDir = parsedJson.getOrElse("parentDir", "/home/hamid/QAL/QP/SDL/")
    Proteus_URL = parsedJson.getOrElse("ProteusJDBC_URL", "")
    Proteus_username = parsedJson.getOrElse("ProteusUsername", "")
    Proteus_pass = parsedJson.getOrElse("ProteusPassword", "")
    REST_PORT = parsedJson.getOrElse("port", "4545").toInt
    pathToQueryLog = parentDir + "log.txt"
    pathToTableCSV = parentDir + "data_csv/"
    pathToSketches = parentDir + "materializedSketches/"
    pathToTableParquet = parentDir + "data_parquet/"
    pathToSaveSynopses = parentDir + "materializedSynopses/"

    if (new java.io.File(parentDir + "warehouseParquetNameToSize.ser").exists)
      new ObjectInputStream(new FileInputStream(parentDir + "warehouseParquetNameToSize.ser")).readObject.asInstanceOf[mutable.HashMap[String, Long]].foreach((a) => warehouseParquetNameToSize.put(a._1, a._2))
    if (new java.io.File(parentDir + "ParquetNameToSynopses.ser").exists)
      new ObjectInputStream(new FileInputStream(parentDir + "ParquetNameToSynopses.ser")).readObject.asInstanceOf[mutable.HashMap[String, String]].foreach((a) => ParquetNameToSynopses.put(a._1, a._2))
    if (new java.io.File(parentDir + "SynopsesToParquetName.ser").exists)
      new ObjectInputStream(new FileInputStream(parentDir + "SynopsesToParquetName.ser")).readObject.asInstanceOf[mutable.HashMap[String, String]].foreach((a) => SynopsesToParquetName.put(a._1, a._2))
    if (new java.io.File(parentDir + "lastUsedOfParquetSample.ser").exists)
      new ObjectInputStream(new FileInputStream(parentDir + "lastUsedOfParquetSample.ser")).readObject.asInstanceOf[mutable.HashMap[String, Long]].foreach((a) => lastUsedOfParquetSample.put(a._1, a._2))
    if (new java.io.File(parentDir + "parquetNameToHeader.ser").exists)
      new ObjectInputStream(new FileInputStream(parentDir + "parquetNameToHeader.ser")).readObject.asInstanceOf[mutable.HashMap[String, String]].foreach((a) => parquetNameToHeader.put(a._1, a._2))
    if (new java.io.File(parentDir + "sampleToOutput.ser").exists)
      new ObjectInputStream(new FileInputStream(parentDir + "sampleToOutput.ser")).readObject.asInstanceOf[mutable.HashMap[String, Seq[Attribute]]].foreach((a) => sampleToOutput.put(a._1, a._2))

  }

  def updateWarehouse(future: Seq[String]): Unit = {
    println(future)
    val warehouseSynopsesToSize = warehouseParquetNameToSize.map(x => (ParquetNameToSynopses(x._1), x._2))
    if (future.size == 0 || warehouseSynopsesToSize.size == 0 || warehouseSynopsesToSize.reduce((a, b) => (null, a._2 + b._2))._2 <= maxSpace * 10)
      return
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
        0 //costOfPlan(physicalPlan, candidateSynopses)._2 - costOfPlan(physicalPlan, candidateSynopses ++ Seq(synopsisInWarehouse))._2
      }).reduce((A1, A2) => if (A1 > A2) A1 else A2)
    ).reduce(_ + _))).map(x => (x._1._1, x._2)).toList.sortBy(_._2).reverse
    //println("best is:\n"+bestSynopsis)
    // create a list of synopses for keeping, gradually
    var index = 0
    var bestSynopsis = p(index)
    var bestSynopsisSize = warehouseSynopsesToSize.getOrElse(bestSynopsis._1, 0.toLong)
    val removeSynopses = new mutable.HashSet[String]()
    while (index < warehouseSynopsesToSize.size - 1) {
      if (candidateSynopsesSize + bestSynopsisSize < maxSpace * 10) {
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
      (Directory(new File(pathToSaveSynopses + parquetName + ".obj"))).deleteRecursively()
      warehouseParquetNameToRow.remove(parquetName)
      warehouseParquetNameToSize.remove(parquetName)
      SynopsesToParquetName.remove(x)
      ParquetNameToSynopses.remove(parquetName)
      parquetNameToHeader.remove(parquetName)
      lastUsedOfParquetSample.remove(parquetName)
      sampleToOutput.remove(parquetName)
      println("removed:    " + x)
      numberOfRemovedSynopses += 1
    })
  }

  def enumerateRawPlanWithJoin(rawPlan: LogicalPlan): Seq[LogicalPlan] = {
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

}
