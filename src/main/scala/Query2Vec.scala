import main.loadTables
import java.io.{File, FileOutputStream, ObjectOutputStream, PrintWriter}
import definition.Paths
import org.apache.spark.sql.SparkSession
import definition.Paths._

object Query2Vec {
  val pathToQueryLog = "/home/hamid/TASTER/spark-data/skyServer/queryLogs/pp" //args(0)
  val parentDir = "/home/hamid/skyServerVectors/"
  val resultPath = parentDir + "vectorsIP_" + Paths.ACCESSED_COL_MIN_FREQUENCY + "_" + Paths.MAX_NUMBER_OF_QUERY_REPETITION
  val resultInfoPath = resultPath + "Info"
  val bench = "skyServer"
  val dataDir = "/home/hamid/TASTER/spark-data/skyServer/sf1/data_parquet/"
  val sparkSession = SparkSession.builder
    .appName("Query2Vec")
    .master("local[*]")
    .getOrCreate();

  def main(args: Array[String]): Unit = {
    import sparkSession.implicits._
    SparkSession.setActiveSession(sparkSession)
    sparkSession.sparkContext.setLogLevel("ERROR");
    loadTables(sparkSession, bench, dataDir)
    val queryLog = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ";").option("nullValue", "null").schema(logSchema).load(pathToQueryLog)
    val queriesStatement = queryLog.filter("statement is not null").filter(row => row.getAs[String]("statement").trim.length > 0)
      .sort("clientIP", "yy", "mm", "dd", "hh", "mi", "ss", "seq").select("statement").map(_.getAs[String](0)).collect()
    val (vectors, accessedColToVectorIndex, groupByKeyToVectorIndex, joinKeyToVectorIndex, accessedColFRQ, groupByFRQ, joinKeyFRQ) = queryToVector(queriesStatement, sparkSession)
    val vectorSize = vectors.size
    var counter = 0
    var writer = new PrintWriter(new File(resultPath))
    var pre = vectors(0)
    var emptyVectorCNT = 0
    var repetitiveCNT = 0
    for (vector <- vectors) if (vector.contains("1")) {
      if (vector.equals(pre))
        counter += 1
      else
        counter = 0
      if (counter < MAX_NUMBER_OF_QUERY_REPETITION)
        writer.println(vector)
      else
        repetitiveCNT += 1
      pre = vector
    }
    else
      emptyVectorCNT += 1
    writer.close()
    var oos: ObjectOutputStream = new ObjectOutputStream(new FileOutputStream(resultPath + "_accessedColToVectorIndex.ser"))
    oos.writeObject(accessedColToVectorIndex)
    oos.close()
    oos = new ObjectOutputStream(new FileOutputStream(resultPath + "_groupByKeyToVectorIndex.ser"))
    oos.writeObject(groupByKeyToVectorIndex)
    oos.close()
    oos = new ObjectOutputStream(new FileOutputStream(resultPath + "_joinKeyToVectorIndex.ser"))
    oos.writeObject(joinKeyToVectorIndex)
    oos.close()
    writer = new PrintWriter(new File(resultInfoPath))
    writer.println("empty Vectors: " + emptyVectorCNT)
    writer.println("repetitive Queries: " + repetitiveCNT)
    writer.println("Feature Count: " + vectorSize)
    writer.println("*** AccessedColIndex")
    for (x <- accessedColToVectorIndex.toList.sortBy(_._2))
      writer.println(x._1 + "#" + x._2)
    writer.println("*** GroupByIndex")
    for (x <- groupByKeyToVectorIndex.toList.sortBy(_._2))
      writer.println(x._1 + "#" + x._2)
    writer.println("*** JoinKeyIndex")
    for (x <- joinKeyToVectorIndex.toList.sortBy(_._2))
      writer.println(x._1 + "#" + x._2)
    writer.println("*** AccessedColFrequency")
    for (x <- accessedColFRQ.toList.sortBy(_._2).reverse)
      writer.println(x._1 + "#" + x._2)
    writer.println("*** GroupByFrequency")
    for (x <- groupByFRQ.toList.sortBy(_._2).reverse)
      writer.println(x._1 + "#" + x._2)
    writer.println("*** JoinKeyFrequency")
    for (x <- joinKeyFRQ.toList.sortBy(_._2).reverse)
      writer.println(x._1 + "#" + x._2)
    writer.close()
    println("empty Vectors: " + emptyVectorCNT)
    println("repetitive Queries: " + repetitiveCNT)
    println("Feature Count: " + vectorSize)
    println(accessedColFRQ)
    println(groupByFRQ)
    println(joinKeyFRQ)
    println(accessedColToVectorIndex)
    println(groupByKeyToVectorIndex)
    println(joinKeyToVectorIndex)
  }
}
