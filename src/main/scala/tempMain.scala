import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import rules.physical.SampleTransformation2

object tempMain {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .appName("Taster")
      .master("local[*]")
      .getOrCreate();
    sparkSession.experimental.extraStrategies = Seq(SampleTransformation2);
    import sparkSession.implicits._
    val DATA_DIR = "/home/hamid/TASTER/spark-data/atoka/sf1/data_parquet/"
    val BRS10K = sparkSession.read.parquet(DATA_DIR + "SCV3.parquet");
    sparkSession.sqlContext.createDataFrame(BRS10K.rdd, BRS10K.schema).createOrReplaceTempView("Table");
    val folder = (new File("/home/hamid/temp/")).listFiles.filter(_.isDirectory)
    for (i <- 0 to folder.size - 1) {
      if (!folder(i).getName.contains(".parquet") && !folder.find(_.getName == folder(i).getName + ".parquet").isDefined) {

        println("asd")
      }
    }
    // val x=sparkSession.sqlContext.sql("select lat,lon from BRS10K ").queryExecution.executedPlan
    // println(x)
    // sparkSession.sqlContext.sql("select lat,lon from BRS10K where id is not null").show(20000)

    sparkSession.sqlContext.sql("select * from Table").show(20000)
    //  sparkSession.sqlContext.sql("select * from Table").show(20000)

    //(x.children(0).children(0).children(0)).execute().saveAsTextFile("/home/hamid/temp/asd")

  }
}
/*val rdd=sparkSession.sparkContext.textFile("/home/hamid/BRS10K.csv")
rdd.saveAsObjectFile("/home/hamid/BRS10Ktemp.csv")
val rdd2=SparkContext.getOrCreate().objectFile[String]("/home/hamid/BRS10Ktemp.csv")
rdd2.take(10).foreach(println)*/