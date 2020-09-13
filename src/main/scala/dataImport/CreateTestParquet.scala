package dataImport

import org.apache.spark.sql.SparkSession

/**
 * Main for running random queries
 * Par. 1 : DROP & LOAD (0 do not drop & load, 1 for drop and load, 2 for drop and load samples)
 */
object CreateTestParquet {

  def main(args: Array[String]) {
    val appName = "Run";
    val sparkSession = SparkSession.builder
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/sparksql_warehouse/olma/")
      .enableHiveSupport()
      .getOrCreate();

    sparkSession.sparkContext.setLogLevel("ERROR");
    sparkSession.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkSession.conf.set("spark.sql.codegen.wholeStage", false);

    val sf = 1 //args(1);
    // hdfs or local
    val hdfsOrLocal = "local" //args(2);
    val format = "csv";
    var PARENT_DIR = ""
    if (hdfsOrLocal.equals("local")) {
      PARENT_DIR = "/home/hamid/TASTER/spark-data/test/sf" + sf + "/";
    } else if (hdfsOrLocal.equals("hdfs")) {
      PARENT_DIR = "hdfs://145.100.59.58:9000/test/sf" + sf + "/";
    }

    val DATA_DIR = PARENT_DIR + "data_" + format + "/";

    val tab1 = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ";").option("nullValue", "null").load(DATA_DIR + "test_tab1.csv")
    sparkSession.sqlContext.createDataFrame(tab1.rdd, tab1.schema).createOrReplaceTempView("tab1");
    tab1.write.format("parquet").save(PARENT_DIR + "data_parquet/tab1.parquet");
    val tab2 = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ";").option("nullValue", "null").load(DATA_DIR + "test_tab2.csv")
    sparkSession.sqlContext.createDataFrame(tab2.rdd, tab2.schema).createOrReplaceTempView("tab2");
    tab2.write.format("parquet").save(PARENT_DIR + "data_parquet/tab2.parquet");
  }
}
