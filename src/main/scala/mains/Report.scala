package mains

import java.io.{File, PrintWriter}

import definition.Paths._
import mains.density_cluster.sparkSession
import sparkSession.implicits._

import scala.collection.Seq

object Report extends QueryEngine_Abs("Report") {
  val parentDir = "/home/hamid/AAQP_Charts/"
  //val pathToReportFile = "/home/hamid/IdeaProjects/PredictionModel/MLResultSingleTime.txt"
  val pathToReportFile = "/home/hamid/IdeaProjects/PredictionModel/MLResult.txt"
  val pathToReportFile2 = "/home/hamid/IdeaProjects/PredictionModel/MLResultWeighted.txt"
  val setup: Seq[(String, Long, Long)] = Seq(("2WeekTo2Week", 2 * WEEK, 2 * WEEK)
    , ("1MonthTo1Month", 1 * MONTH, 1 * MONTH), ("2MonthTo2Month", 2 * MONTH, 2 * MONTH)
    , ("3MonthTo3Month", 3 * MONTH, 3 * MONTH), ("6MonthTo6Month", 6 * MONTH, 6 * MONTH)
    , ("1YearTo1Year", 1 * YEAR, 1 * YEAR), ("2YearTo2Year", 2 * YEAR, 2 * YEAR)
    , ("4YearTo4Year", 4 * YEAR, 4 * YEAR), ("2YearTo3Month", 2 * YEAR, 3 * MONTH)
    , ("2YearTo6Month", 2 * YEAR, 6 * MONTH), ("2YearTo1Year", 2 * YEAR, 1 * YEAR)
    , ("2YearTo2Year", 2 * YEAR, 2 * YEAR), ("4YearTo3Month", 4 * YEAR, 3 * MONTH)
    , ("4YearTo6Month", 4 * YEAR, 6 * MONTH), ("4YearTo1Year", 4 * YEAR, 1 * YEAR)
    , ("4YearTo2Year", 4 * YEAR, 2 * YEAR), ("BeginTo6Month", 20 * YEAR, 6 * MONTH))


  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  def main(args: Array[String]): Unit = {
    //  val records = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "false")
    //    .option("delimiter", "_").schema(reportSchema).load(pathToReportFile)
    val view = sparkSession.read.format("com.databricks.spark.csv").option("header", "false")
      .option("delimiter", "_").schema(reportSchema).load(pathToReportFile)
    sparkSession.sqlContext.createDataFrame(view.rdd, view.schema).createOrReplaceTempView("Report")
    val view2 = sparkSession.read.format("com.databricks.spark.csv").option("header", "false")
      .option("delimiter", "_").schema(reportSchema2).load(pathToReportFile2)
    sparkSession.sqlContext.createDataFrame(view2.rdd, view2.schema).createOrReplaceTempView("Report2")
    val x = view.filter("predictionSize==10").select("tag","model").map(r=>(r.getString(0),r.getString(1).drop(1).toInt)).groupBy("_1").max()
      .map(r => (r.getString(0), r.getInt(1).toInt)).collect().toMap
    x.foreach(println)
    val tuples=view.filter("predictionSize==10").select("tag", "model", "fMeasure")
      .map(r => (r.getString(0), (r.getString(1).drop(1).toInt*13 / x.get(r.getString(0)).get.toFloat), r.getInt(2)))
    for (ent <- x) {
      var writer = new PrintWriter(new File(parentDir+ent._1))
      val t=tuples.filter(x=>x._1.equalsIgnoreCase(ent._1)).collect()
      //writer.println("0.0,"+t(0)._3)
      t.foreach(x=>writer.println(x._2+","+x._3))
      writer.close()

    }
    sparkSession.sql("select tag,model,predictionSize, count(*) from Report where predictionSize==10 group by tag,predictionSize,model having count(*)>1 order by tag").show(10000)

    sparkSession.sql("select tag,dnn,predictionSize,count(*), sum(time),avg(time) from Report where dnn == \"LSTM\"  group by dnn,tag,predictionSize  order by predictionSize,tag").show(10000)

    sparkSession.sqlContext.sql("Select tag, dnn, predictionSize, avg(fMeasure), avg(precision), avg(recall)" +
      ", avg(accuracy) from Report where predictionSize == 5 group by tag, dnn, predictionSize order by avg(fMeasure),tag, dnn, predictionSize").show(10000)

    sparkSession.sqlContext.sql("Select tag, dnn, predictionSize, sum(fMeasure*c)/sum(c)" +
      ", avg(accuracy) from Report2 where predictionSize == 10 group by tag, dnn, predictionSize order by tag, dnn, predictionSize").show(10000)

    sparkSession.sqlContext.sql("Select tag, dnn, predictionSize, avg(fMeasure), avg(precision), avg(recall)" +
      ", avg(accuracy) from Report where predictionSize == 10 group by tag, dnn, predictionSize order by tag, dnn, predictionSize").show(10000)

    sparkSession.sqlContext.sql("Select tag, dnn, predictionSize, avg(fMeasure), avg(precision), avg(recall)" +
      ", avg(accuracy) from Report where predictionSize == 3 group by tag, dnn, predictionSize order by tag, dnn, predictionSize").show(10000)

    sparkSession.sqlContext.sql("Select tag, dnn, predictionSize, avg(fMeasure), avg(precision), avg(recall)" +
      ", avg(accuracy) from Report where predictionSize == 15 group by tag, dnn, predictionSize order by avg(fMeasure),tag, dnn, predictionSize").show(10000)
  }

  override def readConfiguration(args: Array[String]): Unit = Unit

  override def ReadNextQueries(query: String, ip: String, epoch: Long, queryIndex: Int): Seq[String] = null

}
