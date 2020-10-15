import java.io.{BufferedReader, File, FileOutputStream, FileReader, FilenameFilter, PrintWriter}
import java.net.ServerSocket

import definition.TableDefs
import extraSQLOperators.extraSQLOperators
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.{DataFrame, SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project, ReturnAnswer, Sort, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{CollapseCodegenStages, FilterExec, LogicalRDD, PlanLater, PlanSubqueries, ProjectExec, RDDScanExec, ReuseSubquery, SparkPlan}
import rules.logical.{ApproximateInjector, pushFilterUp}
import rules.physical.{ChangeSampleToScan, SampleTransformation, ScaleAggregateSampleExec}

import scala.collection.{Seq, mutable}
import scala.collection.mutable.ListBuffer
import scala.io.Source
import java.util

import operators.physical.{DistinctSampleExec2, SampleExec, UniformSampleExec2, UniformSampleExec2WithoutCI, UniversalSampleExec2}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReuseExchange}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.types.BooleanType

import scala.reflect.io.Directory

object main {
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

  val sparkSession = SparkSession.builder
    .appName("Taster")
    .master("local[*]")
    .getOrCreate();


  val parentDir = "/home/hamid/TASTER/"
  // val parentDir="/home/sdlhshah/spark-data/"
  val windowSize = 5
  val pathToSaveSynopses = parentDir + "materializedSynopsis/"
  val pathToSaveSchema = parentDir + "materializedSchema/"
  val pathToSynopsesFileName = parentDir + "SynopsesToFileName.txt"
  val pathToCIStats = parentDir + "CIstats/"
  val pathToTableSize = parentDir + "tableSize.txt"
  val delimiterSynopsesColumnName = "#"
  val delimiterSynopsisFileNameAtt = ";"
  val delimiterParquetColumn = ","
  val startSamplingRate = 5
  val stopSamplingRate = 50
  val samplingStep = 5
  val maxSpace = 100000000

  def preparations: Seq[Rule[SparkPlan]] = Seq(
    PlanSubqueries(sparkSession),
    EnsureRequirements(sparkSession.sessionState.conf),
    CollapseCodegenStages(sparkSession.sessionState.conf),
    ReuseExchange(sparkSession.sessionState.conf),
    ReuseSubquery(sparkSession.sessionState.conf),
    ChangeSampleToScan(sparkSession, pathToSynopsesFileName, delimiterSynopsisFileNameAtt, delimiterSynopsesColumnName)
  )


  var mapRDDScanSize: mutable.HashMap[RDDScanExec, Long] = new mutable.HashMap[RDDScanExec, Long]()

  def main(args: Array[String]): Unit = {

    SparkSession.setActiveSession(sparkSession)

    System.setProperty("geospark.global.charset", "utf8")
    sparkSession.sparkContext.setLogLevel("ERROR");
    sparkSession.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkSession.conf.set("spark.driver.maxResultSize", "8g")
    sparkSession.conf.set("spark.sql.codegen.wholeStage", false); // disable codegen
    val seed = 5427500315423L
    val (bench, format, run, plan, option, repeats, currendDir, parentDir, benchDir, dataDir) = analyzeArgs(args);
    val queries = queryWorkload(bench, benchDir)
    loadTables(sparkSession, bench, dataDir)
    mapRDDScanSize = readRDDScanSize(dataDir, pathToSaveSynopses)
    val (extraOpt, extraStr) = setRules(option)
    /*sparkSession.sqlContext.sql("select count(numberOfEmployees),numberOfEmployees from SCV s group by numberOfEmployees").show(1000)
    sparkSession.sqlContext.sql("select count(*) from SCV s, PFV p where s.acheneID==p.company_acheneID and numberOfEmployees>0 ").show(1000)
    sparkSession.sqlContext.sql("select count(*) from SCV s, PFV p where s.acheneID==p.company_acheneID and numberOfEmployees>1 ").show(1000)
    sparkSession.sqlContext.sql("select count(*) from SCV s, PFV p where s.acheneID==p.company_acheneID and numberOfEmployees>2 ").show(1000)
    sparkSession.sqlContext.sql("select count(*) from SCV s, PFV p where s.acheneID==p.company_acheneID and numberOfEmployees>3 ").show(1000)
    throw new Exception("Done")*/
    val server = new ServerSocket(4545)
    // println("Server initialized:")
    // while (true) {
    for (i <- 0 to queries.size - 1) {
      val query = queries(i)
      var outString = ""
      /*val clientSocket = server.accept()
      val input = clientSocket.getInputStream()
      val output = clientSocket.getOutputStream()
      var query = java.net.URLDecoder.decode(new BufferedReader(new InputStreamReader(input)).readLine, StandardCharsets.UTF_8.name)
      println(query)
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
      convertSampleTextToParquet(sparkSession)
      updateSynopsesView(sparkSession)
      //   sparkSession.sql("select count(1),revenue from samplefRCcnsWqQAJiDmwhHEoS group by revenue").show(10000)

      var (query_code, confidence, error, dataProfileTable, quantileCol, quantilePart, binningCol, binningPart
      , binningStart, binningEnd, table, tempQuery) = tokenizeQuery(query)
      sparkSession.experimental.extraOptimizations = Seq(new ApproximateInjector(confidence, error, seed), new pushFilterUp);
      sparkSession.experimental.extraStrategies = extraStr;
      //val p=sparkSession.sqlContext.sql("select count(s.legalStatus),s.legalStatus from PFV p, SCV s where p.company_acheneID=s.acheneID  group by s.legalStatus").queryExecution.executedPlan

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
        //  val l=sparkSession.sessionState.catalog.lookupRelation(new  org.apache.spark.sql.catalyst.TableIdentifier("SCV",None))
        // val logicalPlanToTable: mutable.HashMap[String, String] = new mutable.HashMap()
        // recursiveProcess(sparkSession.sqlContext.sql(query_code).queryExecution.analyzed, logicalPlanToTable)
        //   sparkSession.sqlContext.sql(query_code).queryExecution.logical
        //   val sampleParquetToTable: mutable.HashMap[String, String] = new mutable.HashMap()
        //   val sampleRate: mutable.HashSet[Double] = new mutable.HashSet[Double]()
        val rawPlans = enumerateRawPlanWithJoin(query_code)
        val logicalPlans = rawPlans.map(x => sparkSession.sessionState.optimizer.execute(x))
        val physicalPlans = logicalPlans.flatMap(x => sparkSession.sessionState.planner.plan(ReturnAnswer(x)))
        val costOfPhysicalPlan = physicalPlans.map(x => (x, costOfPlan(x, Seq()))).sortBy(_._2._2)
        costOfPhysicalPlan.foreach(println)
        println("cheapest plan before execution preparation:")
        println(costOfPhysicalPlan(0)._1)
        val cheapestPhysicalPlan = costOfPhysicalPlan(0)._1.map(prepareForExecution).toList(0)
        println("cheapest plan:")
        println(cheapestPhysicalPlan)
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
        val col = sparkSession.sqlContext.sql(query_code).columns
        val minNumberOfOcc = 15
        val partCNT = 15
        val fraction = 1 / 1 // sampleRate.toList(0)

        println(cheapestPhysicalPlan)
        outString = "[" + cheapestPhysicalPlan.executeCollectPublic().map(row => {
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
        }).mkString(",\n") + "]"
        updateWarehouse(queries.slice(i, i + windowSize))
      }
      println(outString)
      println("Execution time: " + (System.nanoTime() - time) / 100000000)
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
    }
  }

  def updateWarehouse(future: Seq[String]): Unit = {
    // future approximate physical plans
    val rawPlansPerQuery = future.map(enumerateRawPlanWithJoin(_))
    val logicalPlansPerQuery = rawPlansPerQuery.map(x => x.map(sparkSession.sessionState.optimizer.execute(_)))
    val physicalPlansPerQuery = logicalPlansPerQuery.map(x => x.flatMap(y => sparkSession.sessionState.planner.plan(ReturnAnswer(y))))
    //  val costOfPhysicalPlan=physicalPlans.map(x=>(x,costOfPlan(x,Seq()))).sortBy(_._2._2)

    // current synopses and their size
    val source = Source.fromFile(pathToSynopsesFileName)
    val ParquetNameToSynopses = mutable.HashMap[String, Array[String]]()
    val SynopsesToParquetName = mutable.HashMap[Array[String], String]()
    for (line <- source.getLines()) {
      ParquetNameToSynopses.put(line.split(",")(0), line.split(",")(1).split(delimiterSynopsisFileNameAtt))
      SynopsesToParquetName.put(line.split(",")(1).split(delimiterSynopsisFileNameAtt), line.split(",")(0))
    }
    val foldersOfParquetTable = new File(pathToSaveSynopses).listFiles.filter(_.isDirectory)
    var warehouseSynopsesToSize = mutable.HashMap[Array[String], Long]()
    foldersOfParquetTable.foreach(x => warehouseSynopsesToSize.put(ParquetNameToSynopses(x.getName.split("\\.")(0)), folderSize(x)))
    if (warehouseSynopsesToSize.reduce((a, b) => (null, a._2 + b._2))._2 <= maxSpace)
      return Unit
    val candidateSynopses = new ListBuffer[(Array[String], Long)]()
    var candidateSynopsesSize: Long = 0
    var bestSynopsis = warehouseSynopsesToSize.map(x => (x, physicalPlansPerQuery.map(physicalPlans =>
      physicalPlans.map(physicalPlan => costOfPlan(physicalPlan, Seq())._2 - costOfPlan(physicalPlan, candidateSynopses ++ Seq(x))._2).reduce((A1, A2) => if (A1 < A2) A1 else A2)
    ).reduce(_ + _))).map(x => (x._1._1, x._2)).reduce((A1, A2) => if (A1._2 < A2._2) A1 else A2)
    // create a list of synopses for keeping, gradually
    var bestSynopsisSize=warehouseSynopsesToSize.getOrElse(bestSynopsis._1, 0.toLong)
    while (candidateSynopsesSize + bestSynopsisSize < maxSpace) {
      candidateSynopsesSize += bestSynopsisSize
      warehouseSynopsesToSize.remove(bestSynopsis._1)
      bestSynopsis = warehouseSynopsesToSize.map(x => (x, physicalPlansPerQuery.map(physicalPlans =>
        physicalPlans.map(physicalPlan => costOfPlan(physicalPlan, Seq())._2 - costOfPlan(physicalPlan, candidateSynopses ++ Seq(x))._2).reduce((A1, A2) => if (A1 < A2) A1 else A2)
      ).reduce(_ + _))).map(x => (x._1._1, x._2)).reduce((A1, A2) => if (A1._2 < A2._2) A1 else A2)
      bestSynopsisSize=warehouseSynopsesToSize.getOrElse(bestSynopsis._1, 0.toLong)
    }
    // remove the rest
    warehouseSynopsesToSize.foreach(x => {
      (Directory(new File(pathToSaveSynopses + SynopsesToParquetName.getOrElse(x._1, "null")))).deleteRecursively()
      (Directory(new File(pathToSaveSchema + SynopsesToParquetName.getOrElse(x._1, "null")))).deleteRecursively()
      SynopsesToParquetName.remove(x._1)
    })
    new PrintWriter(new FileOutputStream(new File(pathToSynopsesFileName), true)) {
      write(SynopsesToParquetName.map(x => x._2 + "," + x._1.mkString(delimiterSynopsisFileNameAtt)).toList.mkString("\n"))
      close
    }
  }

  def enumerateRawPlanWithJoin(queryCode: String): Seq[LogicalPlan] = {
    var rootTemp: ListBuffer[LogicalPlan] = new ListBuffer[LogicalPlan]
    val rawPlan = sparkSession.sqlContext.sql(queryCode).queryExecution.analyzed
    val subQueries = new ListBuffer[SubqueryAlias]()
    val queue = new mutable.Queue[LogicalPlan]()
    var joinConditions: ListBuffer[BinaryExpression] = new ListBuffer[BinaryExpression]()
    queue.enqueue(rawPlan)
    while (!queue.isEmpty) {
      val t = queue.dequeue()
      t match {
        case SubqueryAlias(name, child) =>
          subQueries.+=(t.asInstanceOf[SubqueryAlias])
        case Aggregate(groupingExpressions, aggregateExpressions, child) =>
          queue.enqueue(child)
          rootTemp.+=(Aggregate(groupingExpressions, aggregateExpressions, rawPlan.children(0).children(0)))
        case Filter(conditions, child) =>
          joinConditions.++=(getJoinConditions(conditions))
          queue.enqueue(child)
          rootTemp.+=(Filter(conditions, rawPlan.children(0).children(0)))
        case org.apache.spark.sql.catalyst.plans.logical.Join(
        left: LogicalPlan, right: LogicalPlan, joinType: JoinType,
        condition: Option[Expression]) =>
          queue.enqueue(left)
          queue.enqueue(right)
        case Project(p, c) =>
          queue.enqueue(c)
          rootTemp.+=(Project(p, rawPlan.children(0).children(0)))
        case Sort(order: Seq[SortOrder], global: Boolean, child: LogicalPlan) =>
          queue.enqueue(child)
          rootTemp += Sort(order, global, rawPlan.children(0).children(0))
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
                temp.+=:(Join(allPossibleJoinOrders(j)(l), allPossibleJoinOrders(j)(r), org.apache.spark.sql.catalyst.plans.Inner, None))
        } else if (j < i - j) {
          for (left <- allPossibleJoinOrders(j))
            for (right <- allPossibleJoinOrders(i - j))
              if (!hasTheSameSubquery(left, right)) {
                val jj = (Join(left, right, org.apache.spark.sql.catalyst.plans.Inner, None))
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

  def getJoinConditions(exp: Expression): Seq[BinaryExpression] = exp match {
    case a@And(left, right) =>
      getJoinConditions(left) ++ getJoinConditions(right)
    case o@Or(left, right) =>
      getJoinConditions(left) ++ getJoinConditions(right)
    case b@BinaryOperator(left, right) =>
      if (left.isInstanceOf[AttributeReference] && right.isInstanceOf[AttributeReference])
        return Seq(b)
      Seq()
    case _ =>
      throw new Exception("I was identifying join condition and faced an unknown condition!")
  }

  def IsJoinable(lp1: LogicalPlan, lp2: LogicalPlan, joinConditions: ListBuffer[BinaryExpression]): Boolean = {
    for (i <- 0 to joinConditions.length - 1) {
      val lJoinkey = Set(joinConditions(i).left.toString())
      val rJoinkey = Set(joinConditions(i).right.toString())
      val l = lp1.output.map(x => x.toAttribute.toString()).toList
      println("")
      if ((lJoinkey.subsetOf(lp1.output.map(_.toAttribute.toString()).toSet) && rJoinkey.subsetOf(lp2.output.map(_.toAttribute.toString()).toSet))
        || (lJoinkey.subsetOf(lp2.output.map(_.toAttribute.toString()).toSet) && rJoinkey.subsetOf(lp1.output.map(_.toAttribute.toString()).toSet)))
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

  //catch {
  //  case e: Exception =>
  //  val responseDocument = (e.getMessage).getBytes("UTF-8")

  //val responseHeader = ("HTTP/1.1 200 OK\r\n" + "Content-Type: text/html; charset=UTF-8\r\n" + "Content-Length: " + responseDocument.length + "\r\n\r\n").getBytes("UTF-8")

  //     output.write(responseHeader)
  //     output.write(responseDocument)
  //     input.close()
  //     output.close()
  //  }

  def convertSampleTextToParquet(sparkSession: SparkSession) = {
    val folder = (new File(pathToSaveSynopses)).listFiles.filter(_.isDirectory)
    for (i <- 0 to folder.size - 1) {
      try {
        if (folder(i).getName.contains("sample") && !folder(i).getName.contains("_parquet")
          && !folder.find(_.getName == folder(i).getName + "_parquet").isDefined) {
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
            if (d.columns.size != 0) {
              if (theUnion == null) theUnion = d
              else theUnion = theUnion.union(d)
            }
          }
          theUnion.toDF(header: _*).write.format("parquet").save(pathToSaveSynopses + folder(i).getName + "_parquet");
        }
      }
      catch {
        case _ =>
          val directory = new Directory(new File(folder(i).getAbsolutePath))
          directory.deleteRecursively()
      }
    }
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

  val costOfFilter: Long = 1
  val costOfProject: Long = 1
  val costOfScan: Long = 1
  val costOfJoin: Long = 1
  val filterRatio: Double = 0.9
  val costOfUniformSample: Long = 1
  val costOfUniformWithoutCISample: Long = 1
  val costOfUniversalSample: Long = 1
  val costOfDistinctSample: Long = 1
  val costOfScale: Long = 1
  val HashAggregate: Long = 1

  def costOfPlan(pp: SparkPlan, synopsesCost: Seq[(Array[String], Long)]): (Long, Long) = pp match {
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
      val size: Long = mapRDDScanSize.getOrElse(l, -1)
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
      val (inputSize, childCost) = child.map(x => costOfPlan(x, synopsesCost)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
      (inputSize, costOfScale * inputSize + childCost)
    case h@HashAggregateExec(requiredChildDistributionExpressions, groupingExpressions, aggregateExpressions, aggregateAttributes, initialInputBufferOffset, resultExpressions, child) =>
      val (inputSize, childCost) = costOfPlan(child, synopsesCost)
      (inputSize, HashAggregate * inputSize + childCost)
    case _ =>
      throw new Exception("No cost is defined for the node")
  }

  def CostForReadOrCreateSample(sample: SampleExec, synopsesCost: Seq[(Array[String], Long)]) =
    synopsesCost.find(x => isMoreAccurate(sample, x._1)).map(x => (x._2, costOfScan * x._2)).getOrElse(costOfPlan(sample.child, synopsesCost))


  def isMoreAccurate(sampleExec: SampleExec, synopsisInfo: Array[String]): Boolean = sampleExec match {
    case u@UniformSampleExec2(functions, confidence, error, seed, child) =>
      if (synopsisInfo(0).equals("Uniform") && u.output.map(_.name).toSet.subsetOf(synopsisInfo(1).split(delimiterSynopsesColumnName).toSet)
        && synopsisInfo(2).toDouble >= confidence && synopsisInfo(3).toDouble <= error
        && functions.map(_.toString()).toSet.subsetOf(synopsisInfo(5).split(delimiterSynopsesColumnName).toSet)) true else false
    case d@DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, child) =>
      if (synopsisInfo(0).equals("Distinct") && d.output.map(_.name).toSet.subsetOf(synopsisInfo(1).split(delimiterSynopsesColumnName).toSet)
        && synopsisInfo(2).toDouble >= confidence && synopsisInfo(3).toDouble <= error
        && functions.map(_.toString()).toSet.subsetOf(synopsisInfo(6).split(delimiterSynopsesColumnName).toSet)
        && groupingExpressions.map(_.name.split("#")(0)).toSet.subsetOf(synopsisInfo(7).split(delimiterSynopsesColumnName).toSet))
        true else false
    case u@UniversalSampleExec2(functions, confidence, error, seed, joinKey, child) =>
      if (synopsisInfo(0).equals("Universal") && u.output.map(_.name).toSet.subsetOf(synopsisInfo(1).split(delimiterSynopsesColumnName).toSet)
        && synopsisInfo(2).toDouble >= confidence && synopsisInfo(3).toDouble <= error
        && functions.map(_.toString()).toSet.subsetOf(synopsisInfo(5).split(delimiterSynopsesColumnName).toSet)
        && joinKey.map(_.name.split("#")(0)).toSet.subsetOf(synopsisInfo(6).split(delimiterSynopsesColumnName).toSet))
        true else false
    case u@UniformSampleExec2WithoutCI(seed, child) =>
      if (synopsisInfo(0).equals("UniformWithoutCI") && u.output.map(_.name).toSet.subsetOf(synopsisInfo(1).split(delimiterSynopsesColumnName).toSet))
        true else false
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

  def updateSynopsesView(sparkSession: SparkSession): Unit = {
    val existingView = sparkSession.sqlContext.tableNames()
    val folder = (new File(pathToSaveSynopses)).listFiles.filter(_.isDirectory)
    for (i <- 0 to folder.size - 1) {
      if (folder(i).getName.contains("_parquet") && !existingView.contains(folder(i).getName)) {
        val view = sparkSession.read.parquet(pathToSaveSynopses + folder(i).getName);
        sparkSession.sqlContext.createDataFrame(view.rdd, view.schema).createOrReplaceTempView(folder(i).getName.split("_")(0));
      }
    }
  }

  def folderSize(directory: File): Long = {
    var length: Long = 0
    for (file <- directory.listFiles) {
      if (file.isFile) length += file.length
      else length += folderSize(file)
    }
    length
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
      (Seq(), Seq(/*SketchPhysicalTransformation,*/ new SampleTransformation(sparkSession, readTableSize(sparkSession))))
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

  val threshold = 1000

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

  def setDistinctSampleCI(in: SparkPlan, sampleRate: mutable.HashSet[Double]): Unit = {
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
  }

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

  def readTableSize(sparkSession: SparkSession): mutable.HashMap[LogicalRDD, Long] = {
    val map = mutable.HashMap[LogicalRDD, Long]()
    val source = Source.fromFile(pathToTableSize)
    for (line <- source.getLines())
      map.put(sparkSession.sessionState.catalog.lookupRelation(new org.apache.spark.sql.catalyst.TableIdentifier
      (line.split(",")(0), None)).children(0).asInstanceOf[LogicalRDD], line.split(",")(1).toLong)
    source.close()
    map
  }

  def readRDDScanSize(dataDir: String, pathToSynopses: String): mutable.HashMap[RDDScanExec, Long] = {
    val foldersOfParquetTable = new File(dataDir).listFiles.filter(_.isDirectory) ++ new File(pathToSynopses).listFiles.filter(_.isDirectory)
    val tables = sparkSession.sessionState.catalog.listTables("default").map(t => t.table)
    val map = mutable.HashMap[RDDScanExec, Long]()
    foldersOfParquetTable.filter(x => tables.contains(x.getName.split("\\.")(0).toLowerCase)).map(x => {
      val lRDD = sparkSession.sessionState.catalog.lookupRelation(org.apache.spark.sql.catalyst.TableIdentifier
      (x.getName.split("\\.")(0), None)).children(0).asInstanceOf[LogicalRDD]
      (RDDScanExec(lRDD.output, lRDD.rdd, "ExistingRDD", lRDD.outputPartitioning, lRDD.outputOrdering), folderSize(x) / 1000)
    }).foreach(x => map.put(x._1, x._2))
    map
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