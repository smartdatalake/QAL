package mains

import java.io._
import java.util.Date

import definition.Paths._
import mains.density_cluster.sparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions.col
import sparkSession.implicits._

import scala.collection.mutable.ListBuffer
import scala.collection.{Seq, mutable}

object densityClusterDailyTest extends QueryEngine_Abs("densityClusterDailyTest") {

  override def ReadNextQueries(query: String, ip: String, epoch: Long, queryIndex: Int): Seq[String] = null

  val setup: Seq[(String, Long, Long)] = Seq(("4YearTo1Day", 4 * YEAR, 1 * MONTH), ("4YearTo1Year", 4 * YEAR, 1 * YEAR)
    , ("4YearTo2Year", 4 * YEAR, 2 * YEAR))
  /*("1WeekTo1Week", 1 * OneWEEK, 1 * OneWEEK), ("2YearTo1Year", 2 * YEAR, 1 * YEAR)
    , ("1YearTo1Month", 1 * OneYEAR, 1 * OneMONTH) , ("2YearTo2Week", 2 * OneYEAR, 2 * OneWEEK)
    , ("2YearTo1Month", 2 * OneYEAR, 1 * OneMONTH), ("4YearTo1Month", 4 * OneYEAR, 1 * OneMONTH)
    , ("1MonthTo2Week", 1 * OneMONTH, 2 * OneWEEK), ("3MonthTo2Week", 3 * OneMONTH, 2 * OneWEEK)
    , ("6MonthTo2Week", 6 * OneMONTH, 2 * OneWEEK), ("1YearTo2Week", 1 * OneYEAR, 2 * OneWEEK)
    ,

    ("2MonthTo1Month", 2 * OneMONTH, 1 * OneMONTH), ("3MonthTo1Month", 3 * OneMONTH, 1 * OneMONTH)
    ,("6MonthTo1Month", 6 * OneMONTH, 1 * OneMONTH),
      ("6MonthTo3Month", 6 * OneMONTH, 3 * OneMONTH), ("2YearTo3Month", 2 * OneYEAR, 3 * OneMONTH)
   ("1YearTo3Month", 1 * OneYEAR, 3 * OneMONTH), ("4YearTo3Month", 4 * OneYEAR, 3 * OneMONTH)
  ("4YearTo3Month", 4 * OneYEAR, 3 * OneMONTH), ("1YearTo6Month", 1 * OneYEAR, 6 * OneMONTH)
    , ("4YearTo6Month", 4 * OneYEAR, 6 * OneMONTH), ("4YearTo1Year", 4 * OneYEAR, 1 * OneYEAR)
    , ("4YearTo2Year", 4 * OneYEAR, 2 * OneYEAR), ("2YearTo3Month", 2 * OneYEAR, 3 * OneMONTH)
    , ("2YearTo6Month", 2 * OneYEAR, 6 * OneMONTH),
    , ("2YearTo2Year", 2 * OneYEAR, 2 * OneYEAR)
    , ("2WeekTo2Week", 2 * OneWEEK, 2 * OneWEEK), ("1MonthTo1Month", 1 * OneMONTH, 1 * OneMONTH)
    , ("2MonthTo2Month", 2 * OneMONTH, 2 * OneMONTH), ("3MonthTo3Month", 3 * OneMONTH, 3 * OneMONTH)
    , ("6MonthTo6Month", 6 * OneMONTH, 6 * OneMONTH), ("1YearTo1Year", 1 * OneYEAR, 1 * OneYEAR)
    , ("2YearTo2Year", 2 * OneYEAR, 2 * OneYEAR), ("4YearTo4Year", 4 * OneYEAR, 4 * OneYEAR)
    , ("BeginTo6Month", 20 * OneYEAR, 6 * OneMONTH)*/

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  def main(args: Array[String]): Unit = {

    val begin = new Date(2008, 0, 1, 0, 0, 0).getTime / 1000
    val end = new Date(2021, 0, 1, 0, 0, 0).getTime / 1000
    println(end - begin)
    readConfiguration(args)
    loadTables(sparkSession)

    val queryLog = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "false")
      .option("inferSchema", "true").option("delimiter", ";").option("nullValue", "null").schema(logSchema).load(pathToQueryLog)
    val tuples = queryLog.filter("statement is not null and clientIP is not null")
      .filter(row => row.getAs[String]("statement").trim.length > 0 && row.getAs[String]("clientIP").trim.length > 0)
      .sort(col("clientIP").asc, col("yy").asc, col("mm").asc, col("dd").asc, col("hh").asc, col("mi").asc, col("ss").asc, col("seq").asc)
      .select("clientIP", "yy", "mm", "dd", "hh", "mi", "ss", "statement")
      .map(x => (x.getAs[String](0), new Date(x.getAs[Int](1), x.getAs[Int](2), x.getAs[Int](3)
        , x.getAs[Int](4), x.getAs[Int](5), x.getAs[Int](6)).getTime / 1000, x.getAs[String](7))).collect().map(x =>
      (x._1, x._2, x._3, null /*sparkSession.sqlContext.sql(x._3).queryExecution.analyzed*/ )).toSeq
    //  val oos: ObjectOutputStream = new ObjectOutputStream(new FileOutputStream(parentDir + "workloadPlans.ser"))
    //  oos.writeObject(tuples)
    //  oos.close()
    // val t=new ObjectInputStream(new FileInputStream(parentDir + "workloadPlans.ser")).readObject.asInstanceOf[Seq[(String, Long, String, LogicalPlan)]]

    /*  for ((range, y, x) <- setup) {
        var index = begin + x
        var counter = 1
        while (index < end) {
          val tag = (range + "_M" + counter + "_train_gap" + gap + "_processMinLength" + minProcessLength + "_maxQueryRepetition" + MAX_NUMBER_OF_QUERY_REPETITION + "_featureMinFRQ"
            + ACCESSED_COL_MIN_FREQUENCY + "_reserveFeature" + reserveFeature)
          val resultPath = "/home/hamid/QAL/" + range + "/"

          val train = tuples.filter(r => index - y <= r._2 && r._2 < index)
          var indextest = index
          val t = new ListBuffer[Seq[(String, Long, String, LogicalPlan)]]
          while (indextest + DAY <= index + x) {
            val temp = tuples.filter(r => indextest <= r._2 && r._2 < indextest + DAY)
            if (temp.size > 0)
              t.+=(temp)
            indextest += DAY
          }
          //val test = tuples.filter(r => index <= r._2 && r._2 < index + x).toSeq
          val processesTrain = new ListBuffer[ListBuffer[(String, Long, String, LogicalPlan)]]
          val processesTest = new ListBuffer[ListBuffer[(String, Long, String, LogicalPlan)]]
          val processFRQ = new mutable.HashMap[Int, Int]
          if (train.size > 0 && t.find(_.size > 0).isDefined) {
            var temp = new ListBuffer[(String, Long, String, LogicalPlan)]
            temp.+=(train(0))
            for (i <- 1 to train.size - 1)
              if (train(i)._1.equals(train(i - 1)._1) && 0 <= train(i)._2 - train(i - 1)._2 && train(i)._2 - train(i - 1)._2 <= gap)
                temp.+=(train(i))
              else {
                if (temp.size >= minProcessLength)
                  processesTrain.+=(temp)
                temp = new ListBuffer[(String, Long, String, LogicalPlan)]
                temp.+=(train(i))
              }
            if (temp.size > minProcessLength)
              processesTrain.+=(temp)

            val tttt = t.map(test => {
              val processesTest = new ListBuffer[ListBuffer[(String, Long, String, LogicalPlan)]]
              val temp = new ListBuffer[(String, Long, String, LogicalPlan)]
              temp.+=(test(0))
              for (i <- 1 to test.size - 1)
                if (test(i)._1.equals(test(i - 1)._1) && 0 <= test(i)._2 - test(i - 1)._2 && test(i)._2 - test(i - 1)._2 <= gap)
                  temp.+=(test(i))
                else {
                  if (temp.size >= minProcessLength)
                    processesTest.+=(temp)
                  temp.clear()
                  temp.+=(test(i))
                }
              if (temp.size > minProcessLength)
                processesTest.+=(temp)
              processesTest.toSeq
            }).toSeq
            var writer = new PrintWriter(new File(resultPath + counter + "_t" + (new Date((index - y) * 1000).toString) + "_" + (new Date(index * 1000).toString)))
            processesTrain.foreach(x => x.foreach(writer.println))
            writer.close()
            writer.flush()
            writer = new PrintWriter(new File(resultPath + counter + "_s" + (new Date(index * 1000).toString) + "_" + (new Date((index + x) * 1000).toString)))
            processesTest.foreach(x => x.foreach(writer.println))
            writer.close()
            index += x
            counter += 1
          }
        }
      }*/

    for ((range, y, x) <- setup) {
      val time = System.nanoTime()
      var index = begin + x
      var counter = 1
      while (index < end) {
        val tag = (range + "_M" + counter + "_train_gap" + gap + "_processMinLength" + minProcessLength + "_maxQueryRepetition" + MAX_NUMBER_OF_QUERY_REPETITION + "_featureMinFRQ"
          + ACCESSED_COL_MIN_FREQUENCY + "_reserveFeature" + reserveFeature)
        val tagTest = (range + "_M" + counter + "_test_gap" + gap + "_processMinLength" + minProcessLength + "_maxQueryRepetition" + MAX_NUMBER_OF_QUERY_REPETITION + "_featureMinFRQ"
          + ACCESSED_COL_MIN_FREQUENCY + "_reserveFeature" + reserveFeature)
        val resultPath = "/home/hamid/QAL/DensityCluster/" + tag
        val resultPathTest = "/home/hamid/QAL/DensityCluster2/" + tagTest
        val resultWordPath = "/home/hamid/QAL/DensityCluster/" + "processWord_" + tag
        val resultInfoPath = "/home/hamid/QAL/DensityCluster/" + "Info"
        val chartPath = "/home/hamid/QAL/DensityCluster/" + "chart_" + tag + ".png"

        val train = tuples.filter(r => index - y <= r._2 && r._2 < index)
        var indextest = index
        val t = new ListBuffer[Seq[(String, Long, String, LogicalPlan)]]
        while (indextest + DAY <= index + x) {
          val temp = tuples.filter(r => indextest <= r._2 && r._2 < indextest + DAY)
          if (temp.size > 0)
            t.+=(temp)
          indextest += DAY

        }
        // val test = tuples.filter(r => index <= r._2 && r._2 < index + x).toSeq
        val processesTrain = new ListBuffer[ListBuffer[(String, Long, String, LogicalPlan)]]
        val processesTest = new ListBuffer[ListBuffer[(String, Long, String, LogicalPlan)]]
        val processFRQ = new mutable.HashMap[Int, Int]
        if (train.size > 0 && t.find(_.size>0).isDefined) {
          var temp = new ListBuffer[(String, Long, String, LogicalPlan)]
          temp.+=(train(0))
          for (i <- 1 to train.size - 1)
            if (train(i)._1.equals(train(i - 1)._1) && 0 <= train(i)._2 - train(i - 1)._2 && train(i)._2 - train(i - 1)._2 <= gap)
              temp.+=(train(i))
            else {
              if (temp.size >= minProcessLength)
                processesTrain.+=(temp)
              temp = new ListBuffer[(String, Long, String, LogicalPlan)]
              temp.+=(train(i))
            }
          if (temp.size > minProcessLength)
            processesTrain.+=(temp)

          val tttt = t.map(test => {
            val processesTest = new ListBuffer[ListBuffer[(String, Long, String, LogicalPlan)]]
            val temp = new ListBuffer[(String, Long, String, LogicalPlan)]
            temp.+=(test(0))
            for (i <- 1 to test.size - 1)
              if (test(i)._1.equals(test(i - 1)._1) && 0 <= test(i)._2 - test(i - 1)._2 && test(i)._2 - test(i - 1)._2 <= gap)
                temp.+=(test(i))
              else {
                if (temp.size >= minProcessLength)
                  processesTest.+=(temp)
                temp.clear()
                temp.+=(test(i))
              }
            if (temp.size > minProcessLength)
              processesTest.+=(temp)
            processesTest.toSeq
          }).toSeq
          temp = new ListBuffer[(String, Long, String, LogicalPlan)]


          val (processesVec, processesVecTest, ttttt, accessedColToVectorIndex, groupByKeyToVectorIndex, joinKeyToVectorIndex, tableToVectorIndex
          , accessedColFRQ, groupByFRQ, joinKeyFRQ, tableFRQ, vec2FeatureAndFRQ) = processToVector2(processesTrain, processesTest, tttt, sparkSession)
          if (processesVec.size > 0 && ttttt.find(_.size > 0).isDefined) {
            var writer = new PrintWriter(new File(resultPath))
            util.Random.shuffle(processesVec).foreach(processInVec => writer.println(reduceVectorRepetition(processInVec)))
            writer.close()
            for (i <- (0 to ttttt.size - 1)) {
              writer = new PrintWriter(new File(resultPathTest + "_day" + i))
              util.Random.shuffle(ttttt(i)).foreach(processInVec => writer.println(reduceVectorRepetition(processInVec)))
              writer.close()
            }
            writer = new PrintWriter(new File(resultPathTest))
            util.Random.shuffle(processesVecTest).foreach(processInVec => writer.println(reduceVectorRepetition(processInVec)))
            writer.close()
            processesTrain.map(x => (x(0)._1, x.map(tuple => tuple._3)))
            processesTrain.foreach(x => processFRQ.put(x.size, processFRQ.getOrElse(x.size, 0) + 1))
            writer = new PrintWriter(new File(resultPath + "_colIndex"))
            accessedColToVectorIndex.toList.sortBy(_._2).foreach(x => writer.println(x._1 + delimiterHashMap + x._2))
            writer.close()
            writer = new PrintWriter(new File(resultPath + "_groupIndex"))
            groupByKeyToVectorIndex.toList.sortBy(_._2).foreach(x => writer.println(x._1 + delimiterHashMap + x._2))
            writer.close()
            writer = new PrintWriter(new File(resultPath + "_joinIndex"))
            joinKeyToVectorIndex.toList.sortBy(_._2).foreach(x => writer.println(x._1 + delimiterHashMap + x._2))
            writer.close()
            writer = new PrintWriter(new File(resultPath + "_tableIndex"))
            tableToVectorIndex.toList.sortBy(_._2).foreach(x => writer.println(x._1 + delimiterHashMap + x._2))
            writer.close()
            writer = new PrintWriter(new File(resultPath + "_featureFRQ"))
            (accessedColFRQ.toList ++ groupByFRQ.toList ++ joinKeyFRQ.toList ++ tableFRQ.toList).sortBy(_._2).foreach(x => writer.println(x._1 + delimiterHashMap + x._2))
            writer.close()
            writer = new PrintWriter(new File(resultPath + "_vec2Feature"))
            (vec2FeatureAndFRQ).toList.foreach(x => writer.println(x._1 + delimiterHashMap + x._2))
            writer.close()
            writer = new PrintWriter(new File(resultPath + "_processFRQ"))
            processFRQ.toList.sortBy(_._1).map(x => (x._1, x._2)).foreach(writer.println)
            writer.close()
          } else println(range + " " + y + " " + x + " " + counter + " " + index)
          //XYLineChart(processFRQ.toList.sortBy(_._1).map(x => (x._1, x._2)).take(100)).saveAsPNG(chartPath)

        } else println(range + " " + y + " " + x + " " + counter + " " + index)
        index += x
        counter += 1
      }
      println(range + " " + counter + " " + (System.nanoTime() - time) / 1000000000)
    }
  }

  def vec2word(vec2Feature: mutable.HashMap[String, (String, Long)]) = {
    val vec2FeatureAndWordAndFRQ = new mutable.HashMap[String, (String, String, Long)]()
    var index = 1
    sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "false")
      .option("inferSchema", "true").option("delimiter", "%").option("nullValue", "null").load("resultPath")
      .flatMap(x => x.getAs[String](0).split(";")).distinct().collect().toList.foreach(vec => {
      vec2FeatureAndWordAndFRQ.put(vec, (vec2Feature(vec)._1, "Q" + index, vec2Feature(vec)._2))
      index += 1
    })
    var writer = new PrintWriter(new File("resultWordPath"))
    sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "false")
      .option("inferSchema", "true").option("delimiter", "%").option("nullValue", "null").load("resultPath")
      .map(row => row.getAs[String](0).split(";").map(vec2FeatureAndWordAndFRQ.get(_).get._2).mkString(" ")).collect().foreach(process => writer.println(process))
    writer.close()

    writer = new PrintWriter(new File("resultWordPath" + "_vec2FeatureAndWordAndFRQ"))
    vec2FeatureAndWordAndFRQ.toList.sortBy(_._2._3).reverse.foreach(x => writer.println(x._1 + ":" + x._2._1 + ":" + x._2._2 + ":" + x._2._3))
    writer.close()
  }

  def reduceWordRepetition(str: String): (String, Int) = {
    val tokens = str.split(delimiterToken)
    var out = ""
    var prev = tokens(0)
    var counter = 1
    var denied = 0
    for (word <- tokens) {
      if (word.equals(prev))
        counter += 1
      else
        counter = 1
      if (counter <= MAX_NUMBER_OF_QUERY_REPETITION)
        out += (word + delimiterToken)
      else
        denied += 1
      prev = word

    }
    (out.dropRight(0), denied)
  }

  def reduceVectorRepetition(vecs: Seq[String]): String = {
    var out = ""
    var prev = vecs(0)
    var counter = 0
    var denied = 0
    for (word <- vecs) {
      if (word.equals(prev))
        counter += 1
      else
        counter = 1
      if (counter <= MAX_NUMBER_OF_QUERY_REPETITION)
        out += (word + delimiterVector)
      else
        denied += 1
      prev = word
    }
    out = out.dropRight(1)
    out
  }

  override def readConfiguration(args: Array[String]): Unit

  = {
    parentDir = "/home/hamid/QAL/QP/skyServer/"
    pathToQueryLog = parentDir + "workload/skyServerExecutableCleaned_From20080101_To20200101"
    pathToQueryLog = parentDir + "workload/small.txt"
    pathToTableParquet = parentDir + "data_parquet/"

    //pathToQueryLogTest = parentDir + "workload/skyServerExecutableSorted_2020From"
  }


}
