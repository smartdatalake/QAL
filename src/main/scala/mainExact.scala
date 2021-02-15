/*import main.{IsJoinable, chooseConditionFor, enumerateRawPlanWithJoin, getJoinConditions, hasSubquery, hasTheSameSubquery, sparkSession}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BinaryExpression, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project, ReturnAnswer, Sort, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{CollapseCodegenStages, PlanSubqueries, ReuseSubquery, SparkPlan}
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReuseExchange}

import scala.collection.{Seq, mutable}
import scala.collection.mutable.ListBuffer
import scala.io.Source

object mainExact {
  val sparkSession = SparkSession.builder
    .appName("Planner")
    .master("local[*]")
    .getOrCreate();
  def enumerateRawPlanWithJoin(queryCode: String): Seq[LogicalPlan] = enumerateRawPlanWithJoin(sparkSession.sqlContext.sql(queryCode).queryExecution.analyzed)

  def main(args: Array[String]): Unit = {
    SparkSession.setActiveSession(sparkSession)
    System.setProperty("geospark.global.charset", "utf8")
    sparkSession.sparkContext.setLogLevel("ERROR");
    sparkSession.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkSession.conf.set("spark.driver.maxResultSize", "8g")
    sparkSession.conf.set("spark.sql.codegen.wholeStage", false); // disable codegen
    loadTables(sparkSession, "skyServer", "/home/hamid/TASTER/spark-data/skyServer/sf1/data_parquet/")
    val queries = queryWorkload("skyServer", "/home/hamid/TASTER/spark-data/skyServer/")
    var counterNumberOfRowGenerated = 0

    for (i <- 0 to queries.size - 1) {
      val query = queries(i)
      val analyzed = sparkSession.sqlContext.sql(query).queryExecution.analyzed
      val logicalPlan = sparkSession.sqlContext.sql(query).queryExecution.logical
      val physicalPlan = sparkSession.sqlContext.sql(query).queryExecution.executedPlan
      physicalPlan.executeCollectPublic().foreach(x => counterNumberOfRowGenerated += 1)
    }
  }

  def prepareForExecution(plan: SparkPlan): SparkPlan = {
    preparations.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
  }

  def preparations: Seq[Rule[SparkPlan]] = Seq(
    PlanSubqueries(sparkSession),
    EnsureRequirements(sparkSession.sessionState.conf),
    CollapseCodegenStages(sparkSession.sessionState.conf),
    ReuseExchange(sparkSession.sessionState.conf),
    ReuseSubquery(sparkSession.sessionState.conf)
  )

  def loadTables(sparkSession: SparkSession, bench: String, dataDir: String) = {
    if (bench.contains("skyServer"))
      load_skyServer_tables(sparkSession, dataDir);
    else
      throw new Exception("!!!Invalid dataset!!!")
  }


  def load_skyServer_tables(sparkSession: SparkSession, DATA_DIR: String) = {
    val SpecObjAll = sparkSession.read.parquet(DATA_DIR + "specobjall.parquet").drop(colName = "img");
    sparkSession.sqlContext.createDataFrame(SpecObjAll.rdd, SpecObjAll.schema).createOrReplaceTempView("specobjall");
    val PlateX = sparkSession.read.parquet(DATA_DIR + "plateX.parquet");
    sparkSession.sqlContext.createDataFrame(PlateX.rdd, PlateX.schema).createOrReplaceTempView("plateX");
    val SpecObj = sparkSession.read.parquet(DATA_DIR + "specobj.parquet").drop(colName = "img");
    sparkSession.sqlContext.createDataFrame(SpecObj.rdd, SpecObj.schema).createOrReplaceTempView("specobj");
    val PhotoPrimary = sparkSession.read.parquet(DATA_DIR + "photoprimary.parquet");
    sparkSession.sqlContext.createDataFrame(PhotoPrimary.rdd, PhotoPrimary.schema).createOrReplaceTempView("photoprimary");
    val specphoto = sparkSession.read.parquet(DATA_DIR + "specphoto.parquet");
    sparkSession.sqlContext.createDataFrame(specphoto.rdd, specphoto.schema).createOrReplaceTempView("specphoto");
    val photoobj = sparkSession.read.parquet(DATA_DIR + "photoobj.parquet");
    sparkSession.sqlContext.createDataFrame(photoobj.rdd, photoobj.schema).createOrReplaceTempView("photoobj");
    val PhotoObjAll = sparkSession.read.parquet(DATA_DIR + "photoobjall.parquet");
    sparkSession.sqlContext.createDataFrame(PhotoObjAll.rdd, PhotoObjAll.schema).createOrReplaceTempView("photoobjall");
    val galaxy = sparkSession.read.parquet(DATA_DIR + "galaxy.parquet");
    sparkSession.sqlContext.createDataFrame(galaxy.rdd, galaxy.schema).createOrReplaceTempView("galaxy");
    val GalaxyTag = sparkSession.read.parquet(DATA_DIR + "galaxytag.parquet");
    sparkSession.sqlContext.createDataFrame(GalaxyTag.rdd, GalaxyTag.schema).createOrReplaceTempView("galaxytag");
    val FIRST = sparkSession.read.parquet(DATA_DIR + "first.parquet");
    sparkSession.sqlContext.createDataFrame(FIRST.rdd, FIRST.schema).createOrReplaceTempView("first");
    val Field = sparkSession.read.parquet(DATA_DIR + "field.parquet");
    sparkSession.sqlContext.createDataFrame(Field.rdd, Field.schema).createOrReplaceTempView("field");
    val SpecPhotoAll = sparkSession.read.parquet(DATA_DIR + "specphotoall.parquet");
    sparkSession.sqlContext.createDataFrame(SpecPhotoAll.rdd, SpecPhotoAll.schema).createOrReplaceTempView("specphotoall");
    val sppParams = sparkSession.read.parquet(DATA_DIR + "sppparams.parquet");
    sparkSession.sqlContext.createDataFrame(sppParams.rdd, sppParams.schema).createOrReplaceTempView("sppparams");
    val wise_xmatch = sparkSession.read.parquet(DATA_DIR + "wise_xmatch.parquet");
    sparkSession.sqlContext.createDataFrame(wise_xmatch.rdd, wise_xmatch.schema).createOrReplaceTempView("wise_xmatch");
    val emissionLinesPort = sparkSession.read.parquet(DATA_DIR + "emissionlinesport.parquet");
    sparkSession.sqlContext.createDataFrame(emissionLinesPort.rdd, emissionLinesPort.schema).createOrReplaceTempView("emissionlinesport");
    val wise_allsky = sparkSession.read.parquet(DATA_DIR + "wise_allsky.parquet");
    sparkSession.sqlContext.createDataFrame(wise_allsky.rdd, wise_allsky.schema).createOrReplaceTempView("wise_allsky");
    val galSpecLine = sparkSession.read.parquet(DATA_DIR + "galspecline.parquet");
    sparkSession.sqlContext.createDataFrame(galSpecLine.rdd, galSpecLine.schema).createOrReplaceTempView("galspecline");
    val zooSPec = sparkSession.read.parquet(DATA_DIR + "zoospec.parquet");
    sparkSession.sqlContext.createDataFrame(zooSPec.rdd, zooSPec.schema).createOrReplaceTempView("zoospec");
    val Photoz = sparkSession.read.parquet(DATA_DIR + "photoz.parquet");
    sparkSession.sqlContext.createDataFrame(Photoz.rdd, Photoz.schema).createOrReplaceTempView("photoz");
    val zooNoSpec = sparkSession.read.parquet(DATA_DIR + "zoonospec.parquet");
    sparkSession.sqlContext.createDataFrame(zooNoSpec.rdd, zooNoSpec.schema).createOrReplaceTempView("zoonospec");
    val star = sparkSession.read.parquet(DATA_DIR + "star.parquet");
    sparkSession.sqlContext.createDataFrame(star.rdd, star.schema).createOrReplaceTempView("star");
    val propermotions = sparkSession.read.parquet(DATA_DIR + "propermotions.parquet");
    sparkSession.sqlContext.createDataFrame(propermotions.rdd, propermotions.schema).createOrReplaceTempView("propermotions");
    val stellarmassstarformingport = sparkSession.read.parquet(DATA_DIR + "stellarmassstarformingport.parquet");
    sparkSession.sqlContext.createDataFrame(stellarmassstarformingport.rdd, stellarmassstarformingport.schema).createOrReplaceTempView("stellarmassstarformingport");
    val sdssebossfirefly = sparkSession.read.parquet(DATA_DIR + "sdssebossfirefly.parquet");
    sparkSession.sqlContext.createDataFrame(sdssebossfirefly.rdd, sdssebossfirefly.schema).createOrReplaceTempView("sdssebossfirefly");
    val spplines = sparkSession.read.parquet(DATA_DIR + "spplines.parquet");
    sparkSession.sqlContext.createDataFrame(spplines.rdd, spplines.schema).createOrReplaceTempView("spplines");

    //val XXX = sparkSession.read.parquet(DATA_DIR + "XXX.parquet");
    //sparkSession.sqlContext.createDataFrame(XXX.rdd, XXX.schema).createOrReplaceTempView("XXX");
  }

  def queryWorkload(bench: String, BENCH_DIR: String): List[String] = {
    var temp: ListBuffer[String] = ListBuffer();
    if (bench.equals("skyServer")) {
      var src = Source.fromFile(BENCH_DIR + "queryLog.csv").getLines
      src.take(1).next
      var counter = 0
      for (l <- src) {
        if (!l.contains("photoprofile") && !l.contains("peak/snr") && !l.contains("twomass") && !l.contains("masses") && !l.contains(" & ") && !l.contains("count(*)  p.objid") && !l.contains("count(*) p.objid") && !l.contains("count(*), p.objid") && !l.contains("count(*), where") && !l.contains("count(*),where") && !l.contains("count(*)   where") && !l.contains("count(*)  where") && !l.contains("count(*) where") && !l.contains("st.objid") && !l.contains("stellarmassstarformingport") && !l.contains("thingindex") && !l.contains("0x001") && !l.contains("dr9") && !l.contains("fphotoflags") && !l.contains("avg(dec), from") && !l.contains("emissionlinesport") && !l.contains("stellarmasspassiveport") && !l.contains("s.count(z)") && !l.contains("nnisinside") && !l.contains("petromag_u") && !l.contains("insert") && !l.contains("boss_target1") && !l.contains(" photoobj mode = 1") && !l.contains("and count(z)") && !l.contains("gal.extinction_u") && !l.contains("spectroflux_r") && !l.contains("platex") && !l.contains("0x000000000000ffff") && !l.contains("neighbors") && !l.contains("specline") && !l.contains("specclass")) {
          try {
            if (l.split(";")(8).toLong > 0) {
              if (l.split(';')(9).size > 30)
                temp.+=(l.split(';')(9).replace("bestdr9..", "").replaceAll("\\s{2,}", " ").trim())
            } else
              counter = counter + 1
          }
          catch {
            case e: Exception =>
              println(l)
          }
        }
      }
      println("number of queries: " + temp.size)
      temp.toList
    }
    else
      throw new Exception("Invalid input data benchmark")
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

}*/


import definition.Paths.{counterForQueryRow, counterNumberOfRowGenerated, outputOfQuery, seed, start, testSize}
import main.{analyzeArgs, getAggSubQueries, loadTables, numberOfExecutedSubQuery, queryWorkload, sparkSession, tokenizeQuery}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.ReturnAnswer
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReuseExchange}
import org.apache.spark.sql.execution.{CollapseCodegenStages, PlanSubqueries, ReuseSubquery, SparkPlan}
import rules.logical.{ApproximateInjector, pushFilterUp}

import scala.collection.Seq

object mainExact {
  def main(args: Array[String]): Unit = {
    SparkSession.setActiveSession(sparkSession)
    System.setProperty("geospark.global.charset", "utf8")
    sparkSession.sparkContext.setLogLevel("ERROR");
    sparkSession.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkSession.conf.set("spark.driver.maxResultSize", "8g")
    sparkSession.conf.set("spark.sql.codegen.wholeStage", false); // disable codegen
    sparkSession.conf.set("spark.sql.crossJoin.enabled", true)
    sparkSession.experimental.extraOptimizations = Seq(new pushFilterUp);
    val (bench, benchDir, dataDir) = analyzeArgs(args);
    loadTables(sparkSession, bench, dataDir)
    val queries = queryWorkload(bench, benchDir)
    val timeTotal = System.nanoTime()
    var counterNumberOfRowGenerated = 0
    for (i <- start to queries.size - 1) {
      if (i > start + testSize)
        throw new Exception(outputOfQuery + "executed " + numberOfExecutedSubQuery + " queries in " + (System.nanoTime() - timeTotal) / 1000000000 + ", number of generated rows: " + counterNumberOfRowGenerated)
      val query = queries(i)
      // println(query)
      val t = System.nanoTime()
      val (query_code, confidence, error, dataProfileTable, quantileCol, quantilePart, binningCol, binningPart
      , binningStart, binningEnd, table, tempQuery) = tokenizeQuery(query)
      val subQueries = getAggSubQueries(sparkSession.sqlContext.sql(query_code).queryExecution.analyzed)
      outputOfQuery = ""
      counterForQueryRow = 0
      for (subQuery <- subQueries) {
        val logicalPlans = subQuery.map(x => sparkSession.sessionState.optimizer.execute(x))
        val physicalPlans = logicalPlans.flatMap(x => sparkSession.sessionState.planner.plan(ReturnAnswer(x))).map(prepareForExecution).toList(0)
        physicalPlans.executeCollectPublic().toList.foreach(row => {
          outputOfQuery += row.toString()
          counterForQueryRow += 1
        })
        numberOfExecutedSubQuery += 1
      }
      // println(counterForQueryRow + "," + (System.nanoTime() - t) / 1000000000)
    }
  }

  def prepareForExecution(plan: SparkPlan): SparkPlan = {
    preparations.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
  }

  def preparations: Seq[Rule[SparkPlan]] = Seq(
    PlanSubqueries(sparkSession),
    EnsureRequirements(sparkSession.sessionState.conf),
    CollapseCodegenStages(sparkSession.sessionState.conf),
    ReuseExchange(sparkSession.sessionState.conf),
    ReuseSubquery(sparkSession.sessionState.conf)
  )

}

