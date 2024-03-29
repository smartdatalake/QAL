import definition.Paths._
import definition.Paths
import extraSQL.{extraRulesWithoutSampling, extraSQLOperators}
import main._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReturnAnswer, SubqueryAlias}
import org.apache.spark.sql.execution.{LogicalRDD, SparkPlan}
import rules.logical.{ApproximateInjector, pushFilterUp}
import rules.physical.{SampleTransformation, SketchPhysicalTransformation}
import java.io.FileInputStream
import java.io.ObjectInputStream
import java.io.{BufferedReader, BufferedWriter, File, FileOutputStream, FileWriter, InputStreamReader, ObjectOutputStream}
import java.net.ServerSocket
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}
import scala.collection.mutable.ListBuffer
import scala.collection.{Seq, mutable}
import scala.io.Source
import scala.util.control.Breaks.{break, breakable}

object mainSDL {
  val sparkSession = SparkSession.builder
    .appName("QAL")
    .master("local[*]")
    .getOrCreate();
  val tableCounter = new mutable.HashMap[String, Int]()

  def main(args: Array[String]): Unit = {
    var Proteus_URL = ""
    var Proteus_username = ""
    var Proteus_pass = ""
    SparkSession.setActiveSession(sparkSession)
    System.setProperty("geospark.global.charset", "utf8")
    sparkSession.sparkContext.setLogLevel("ERROR");
    sparkSession.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkSession.conf.set("spark.driver.maxResultSize", "8g")
    sparkSession.conf.set("spark.sql.codegen.wholeStage", false); // disable codegen
    sparkSession.conf.set("spark.sql.crossJoin.enabled", true)
    //readSDLConfiguration()
    loadTables()
//    sparkSession.sql("select count(revenue) from PFT p, SCT s where p.company_acheneID= s.acheneID").show()
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
    mapRDDScanRowCNT = definition.Paths.readRDDScanRowCNT(sparkSession)
    sparkSession.experimental.extraStrategies = Seq(/*extraRulesWithoutSampling,*/ SketchPhysicalTransformation, SampleTransformation);

    val REST=false
    val REST_PORT=0
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
        breakable {
          if (inputHTTP.contains("get /removeproteus?")) {
            Proteus_URL = ""
            Proteus_username = ""
            Proteus_pass = ""
            out = "{'status':200,'message':\"Proteus credential is removed.\"}"
            responseDocument = (out).getBytes("UTF-8")
            responseHeader = ("HTTP/1.1 404 FAIL\r\n" + "Content-Type: text/html; charset=UTF-8\r\n" + "Content-Length: " + responseDocument.length + "\r\n\r\n").getBytes("UTF-8")
          }
          else if (inputHTTP.contains("get /changeproteus?")) {
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
            output.write(responseHeader)
            output.write(responseDocument)
            input.close()
            output.close()
          }
          else if (inputHTTP.contains("get /qal?query=")) {
            val query = inputHTTP.replace("get /qal?query=", "").replace(" http/1.1", "")
            //  try {
            queryLog += (query)
            val past = if (queryLog.size >= windowSize) queryLog.slice(queryLog.size - windowSize, queryLog.size) else queryLog.slice(0, queryLog.size)
            out = executeQuery(query, past.toList)
            responseDocument = (out).getBytes("UTF-8")
            responseHeader = ("HTTP/1.1 200 OK\r\n" + "Content-Type: text/html; charset=UTF-8\r\n" + "Content-Length: " + responseDocument.length + "\r\n\r\n").getBytes("UTF-8")
            output.write(responseHeader)
            output.write(responseDocument)
            input.close()
            output.close()
            /*    }
                catch {
                  case e: Exception =>
                    responseDocument = ("{'status':404,'message':\"" + e.getMessage + "\"}").getBytes("UTF-8")
                    responseHeader = ("HTTP/1.1 404 FAIL\r\n" + "Content-Type: text/html; charset=UTF-8\r\n" + "Content-Length: " + responseDocument.length + "\r\n\r\n").getBytes("UTF-8")
                    output.write(responseHeader)
                    output.write(responseDocument)
                    input.close()
                    output.close()
                }*/
          }
          else {
            out = "{'status':404,'message':\"Invalid REST request!!!\"}"
            responseDocument = (out).getBytes("UTF-8")
            responseHeader = ("HTTP/1.1 404 FAIL\r\n" + "Content-Type: text/html; charset=UTF-8\r\n" + "Content-Length: " + responseDocument.length + "\r\n\r\n").getBytes("UTF-8")
            output.write(responseHeader)
            output.write(responseDocument)
            input.close()
            output.close()
          }
        }
      }
    }
    else {
      val queries = loadWorkload()
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
    sparkSession.experimental.extraOptimizations = Seq(new ApproximateInjector(confidence, error, seed), new pushFilterUp);
    if (confidence >= .99 && error <= .10)
      Paths.fractionInitialize = .55
    else if (confidence >= .90 && error <= .10)
      Paths.fractionInitialize = .5
    else if (confidence >= .90 && error <= .20)
      Paths.fractionInitialize = .45
    else if (confidence >= .80 && error <= .20)
      Paths.fractionInitialize = .4
    else
      Paths.fractionInitialize = .3

    if (quantileCol != "") {
      sparkSession.experimental.extraStrategies = Seq(SketchPhysicalTransformation, SampleTransformation);
      val logicalPlan = sparkSession.sessionState.sqlParser.parsePlan(tempQuery)
      checkAndCreateTable(logicalPlan)
      outString = extraSQLOperators.execQuantile(sparkSession, tempQuery, table, quantileCol, quantilePart, confidence, error, seed)
    } else if (binningCol != "") {
      sparkSession.experimental.extraStrategies = Seq(SketchPhysicalTransformation, SampleTransformation);
      val logicalPlan = sparkSession.sessionState.sqlParser.parsePlan(tempQuery)
      checkAndCreateTable(logicalPlan)
      outString = ""// extraSQLOperators.execBinning(sparkSession, table, binningCol, binningPart, binningStart, binningEnd, confidence, error, seed)
    } else if (dataProfileTable != "") {
      val logicalPlan = sparkSession.sessionState.sqlParser.parsePlan("select * from " + dataProfileTable)
      checkAndCreateTable(logicalPlan)
      sparkSession.experimental.extraStrategies = Seq(SketchPhysicalTransformation, SampleTransformation);
      outString = extraSQLOperators.execDataProfile(sparkSession, dataProfileTable, confidence, error, seed)
    } else {
      try {
        sparkSession.experimental.extraStrategies = Seq(SampleTransformation);
        val logicalPlan = sparkSession.sessionState.sqlParser.parsePlan(query_code)
        checkAndCreateTable(logicalPlan)
        val analyzed = sparkSession.sqlContext.sql(query_code).queryExecution.analyzed
        //choose the best approximate physical plan and create related synopses, presently, the lowest-cost plan
        //////////////////////////////////////////////////////////////////////////////////////////////////////////
        val checkpointForSampleConstruction = System.nanoTime()
        updateAttributeName(analyzed, new mutable.HashMap[String, Int]())
        val pp = sparkSession.sessionState.planner.plan(ReturnAnswer(sparkSession.sessionState.optimizer.execute(analyzed))).toList(0)
        var cheapestPhysicalPlan = changeSynopsesWithScan(pp)
        executeAndStoreSample(cheapestPhysicalPlan)
        cheapestPhysicalPlan = changeSynopsesWithScan(cheapestPhysicalPlan)
       // cheapestPhysicalPlan = prepareForExecution(cheapestPhysicalPlan)
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
    }
    numberOfExecutedSubQuery += 1
    //////////////////////////////////////////////////////////////////////////////////////////////////////////

    //update warehouse, LRU or window based
    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    val checkpointForWarehouseUpdate = System.nanoTime()
    updateWarehouse(future)
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
    tableName.clear()
    outString
  }

  def updateAttributeName(lp: LogicalPlan, tableFrequency: mutable.HashMap[String, Int]): Unit = lp match {
    case SubqueryAlias(identifier, child@LogicalRDD(output, rdd, outputPartitioning, outputOrdering, isStreaming)) =>
      val att = output.toList
      tableFrequency.put(identifier.identifier, tableFrequency.getOrElse(identifier.identifier, 0) + 1)
      for (i <- 0 to output.size - 1)
        tableName.put(att(i).toAttribute.toString().toLowerCase, identifier.identifier + "_" + tableFrequency.get(identifier.identifier).get)
    case a =>
      a.children.foreach(x => updateAttributeName(x, tableFrequency))
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

  def loadTables(): Unit = {
    (new File(pathToTableParquet)).listFiles.filter(_.getName.contains(".parquet")).foreach(table => {
      val view = sparkSession.read.parquet(table.getAbsolutePath);
      sparkSession.sqlContext.createDataFrame(view.rdd, view.schema).createOrReplaceTempView(table.getName.split("\\.")(0).toLowerCase);
    })
  }

  def checkAndCreateTable(lp: LogicalPlan): Unit = lp match {
    case node@org.apache.spark.sql.catalyst.analysis.UnresolvedRelation(table) =>
      val listOfFiles = new File(pathToTableParquet).listFiles
      var getTable = true
      for (j <- 0 until listOfFiles.length)
        if (listOfFiles(j).getName == table.table.toLowerCase + ".parquet") getTable = false
      if (getTable)
        getAndCreateTableFromProteus(table.table.toLowerCase)
    case t =>
      t.children.foreach(child => checkAndCreateTable(child))
  }

  def loadWorkload(): List[String] = {
    val temp: ListBuffer[String] = ListBuffer();
    val src = Source.fromFile(pathToQueryLog).getLines
    src.take(1).next
    for (l <- src)
      temp.+=(l)
    temp.toList
  }

  def getAndCreateTableFromProteus(tableName: String): Unit = {
    System.out.println("fetching table from proteus " + tableName)
    Class.forName("org.apache.calcite.avatica.remote.Driver")
    val connection: Connection = DriverManager.getConnection("Proteus_URL", "Proteus_username", "Proteus_pass")
    val statement = connection.createStatement()
    val rs = statement.executeQuery("select * from " + tableName + " limit " + "JDBCRowLimiter")
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


    //  ProteusJDBC.getCSVfromProteus(tableName, pathToTableCSV)
    val view = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(pathToTableCSV + "/" + tableName + ".csv")
    sparkSession.sqlContext.createDataFrame(view.rdd, view.schema).createOrReplaceTempView(tableName.toLowerCase);
    view.write.format("parquet").save(pathToTableParquet + "/" + tableName.toLowerCase + ".parquet");
    val lRDD = sparkSession.sessionState.catalog.lookupRelation(org.apache.spark.sql.catalyst.TableIdentifier
    (tableName, None)).children(0).asInstanceOf[LogicalRDD]
    // new File(pathToTableCSV + "/" + tableName + ".csv").delete()
    mapRDDScanRowCNT.put(lRDD.output.map(o => tableName + "." + o.name.split("#")(0)).mkString(";").toLowerCase, folderSize(new File(pathToTableParquet + "/" + tableName.toLowerCase + ".parquet")))
  }

}
