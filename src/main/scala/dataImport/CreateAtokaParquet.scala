package dataImport

import org.apache.spark.sql.SparkSession

/**
 * Main for running random queries
 * Par. 1 : DROP & LOAD (0 do not drop & load, 1 for drop and load, 2 for drop and load samples)
 */
object CreateAtokaParquet {

  def main(args: Array[String]) {
    val appName = "Run";
    val sparkSession = SparkSession.builder
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/sparksql_warehouse/hamid/")
      .enableHiveSupport()
      .getOrCreate();

    sparkSession.sparkContext.setLogLevel("ERROR");
    sparkSession.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkSession.conf.set("spark.sql.codegen.wholeStage", false);

    // val sf = 1 //args(1);
    // hdfs or local
    val hdfsOrLocal = args(0);
    val format = "csv";
    var PARENT_DIR = ""
    if (hdfsOrLocal.equals("local")) {
      PARENT_DIR = args(1);
    } else if (hdfsOrLocal.equals("hdfs")) {
      PARENT_DIR = "hdfs://145.100.59.58:9000/" + args(1);
    }

    val DATA_DIR = PARENT_DIR + "data_" + format + "/";


    //val SCV = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
    // .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "SCV.csv")
    // sparkSession.sqlContext.createDataFrame(SCV.rdd, SCV.schema).repartition(13).createOrReplaceTempView("SCV");
    //SCV.write.format("parquet").save(PARENT_DIR + "data_parquet/SCV.parquet");
    val PFV = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "PFV.csv")
    //sparkSession.sqlContext.createDataFrame(PFV.rdd, PFV.schema).repartition(13).createOrReplaceTempView("PFV");
    PFV.write.format("parquet").save(PARENT_DIR + "data_parquet/PFV.parquet");

    // val temp = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
    //    .option("inferSchema", "true").option("delimiter", ";").option("nullValue", "null").load(DATA_DIR + "BRS10K.csv")
    //  temp.write.format("parquet").save(PARENT_DIR + "data_parquet/BRS10K.parquet");
    /*val companies = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "atoka_companies.csv")
    sparkSession.sqlContext.createDataFrame(companies.rdd, companies.schema).createOrReplaceTempView("atoka_companies");
    companies.write.format("parquet").save(PARENT_DIR + "data_parquet/companies.parquet");*/
  }
}
