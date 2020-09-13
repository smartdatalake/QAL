package dataImport

import org.apache.spark.sql.SparkSession

/**
 * Main for running random queries
 * Par. 1 : DROP & LOAD (0 do not drop & load, 1 for drop and load, 2 for drop and load samples)
 */
object CreateSkyServerParquet {

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


    val sf = 1//args(1);

    // hdfs or local
    val hdfsOrLocal = "local"//args(2);

    val format = "csv";

    var PARENT_DIR = ""
    if (hdfsOrLocal.equals("local")) {
      PARENT_DIR = "/home/hamid/TASTER/spark-data/skyServer/sf" + sf + "/";
    } else if (hdfsOrLocal.equals("hdfs")) {
      PARENT_DIR = "hdfs://145.100.59.58:9000/skyServer/sf" + sf + "/";
    }

    val DATA_DIR = PARENT_DIR + "data_" + format + "/";

    val specObjAll = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "SpecObjAll.csv")
    sparkSession.sqlContext.createDataFrame(specObjAll.rdd,specObjAll.schema).createOrReplaceTempView("SpecObjAll");
    specObjAll.write.format("parquet").save(PARENT_DIR + "data_parquet/SpecObjAll.parquet");

    val plateX = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "PlateX.csv")
    sparkSession.sqlContext.createDataFrame(plateX.rdd,plateX.schema).createOrReplaceTempView("PlateX");
    plateX.write.format("parquet").save(PARENT_DIR + "data_parquet/PlateX.parquet");

    val specPhotoAll = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "SpecPhotoAll.csv")
    sparkSession.sqlContext.createDataFrame(specPhotoAll.rdd,specPhotoAll.schema).createOrReplaceTempView("SpecPhotoAll");
    specPhotoAll.write.format("parquet").save(PARENT_DIR + "data_parquet/SpecPhotoAll.parquet");

    val sppParams = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "sppParams.csv")
     sparkSession.sqlContext.createDataFrame(sppParams.rdd,sppParams.schema).createOrReplaceTempView("sppParams");
     sppParams.write.format("parquet").save(PARENT_DIR + "data_parquet/sppParams.parquet");

    val SpecObj = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "SpecObj.csv")
    sparkSession.sqlContext.createDataFrame(SpecObj.rdd,SpecObj.schema).createOrReplaceTempView("SpecObj");
    SpecObj.write.format("parquet").save(PARENT_DIR + "data_parquet/SpecObj.parquet");

    val PhotoPrimary = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "PhotoPrimary.csv")
    sparkSession.sqlContext.createDataFrame(PhotoPrimary.rdd,PhotoPrimary.schema).createOrReplaceTempView("PhotoPrimary");
    PhotoPrimary.write.format("parquet").save(PARENT_DIR + "data_parquet/PhotoPrimary.parquet");

   val specphoto = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
     .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "specphoto.csv")
    sparkSession.sqlContext.createDataFrame(specphoto.rdd,specphoto.schema).createOrReplaceTempView("specphoto");
    specphoto.write.format("parquet").save(PARENT_DIR + "data_parquet/specphoto.parquet");

   val photoobj = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
     .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "photoobj.csv")
    sparkSession.sqlContext.createDataFrame(photoobj.rdd,photoobj.schema).createOrReplaceTempView("photoobj");
    photoobj.write.format("parquet").save(PARENT_DIR + "data_parquet/photoobj.parquet");

   val wise_xmatch = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
     .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "wise_xmatch.csv")
    sparkSession.sqlContext.createDataFrame(wise_xmatch.rdd,wise_xmatch.schema).createOrReplaceTempView("wise_xmatch");
    wise_xmatch.write.format("parquet").save(PARENT_DIR + "data_parquet/wise_xmatch.parquet");

   val PhotoObjAll = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
     .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "PhotoObjAll.csv")
    sparkSession.sqlContext.createDataFrame(PhotoObjAll.rdd,PhotoObjAll.schema).createOrReplaceTempView("PhotoObjAll");
    PhotoObjAll.write.format("parquet").save(PARENT_DIR + "data_parquet/PhotoObjAll.parquet");


val emissionLinesPort = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
     .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "emissionLinesPort.csv")
    sparkSession.sqlContext.createDataFrame(emissionLinesPort.rdd,emissionLinesPort.schema).createOrReplaceTempView("emissionLinesPort");
    emissionLinesPort.write.format("parquet").save(PARENT_DIR + "data_parquet/emissionLinesPort.parquet");

val wise_allsky = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
     .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "wise_allsky.csv")
    sparkSession.sqlContext.createDataFrame(wise_allsky.rdd,wise_allsky.schema).createOrReplaceTempView("wise_allsky");
    wise_allsky.write.format("parquet").save(PARENT_DIR + "data_parquet/wise_allsky.parquet");

val galSpecLine = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
     .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "galSpecLine.csv")
    sparkSession.sqlContext.createDataFrame(galSpecLine.rdd,galSpecLine.schema).createOrReplaceTempView("galSpecLine");
    galSpecLine.write.format("parquet").save(PARENT_DIR + "data_parquet/galSpecLine.parquet");

val zooSPec = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
     .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "zooSPec.csv")
    sparkSession.sqlContext.createDataFrame(zooSPec.rdd,zooSPec.schema).createOrReplaceTempView("zooSPec");
    zooSPec.write.format("parquet").save(PARENT_DIR + "data_parquet/zooSPec.parquet");

val galaxy = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
     .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "galaxy.csv")
    sparkSession.sqlContext.createDataFrame(galaxy.rdd,galaxy.schema).createOrReplaceTempView("galaxy");
    galaxy.write.format("parquet").save(PARENT_DIR + "data_parquet/galaxy.parquet");

val Photoz = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
     .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "Photoz.csv")
    sparkSession.sqlContext.createDataFrame(Photoz.rdd,Photoz.schema).createOrReplaceTempView("Photoz");
    Photoz.write.format("parquet").save(PARENT_DIR + "data_parquet/Photoz.parquet");

val zooNoSpec = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
     .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "zooNoSpec.csv")
    sparkSession.sqlContext.createDataFrame(zooNoSpec.rdd,zooNoSpec.schema).createOrReplaceTempView("zooNoSpec");
    zooNoSpec.write.format("parquet").save(PARENT_DIR + "data_parquet/zooNoSpec.parquet");

val GalaxyTag = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
     .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "GalaxyTag.csv")
    sparkSession.sqlContext.createDataFrame(GalaxyTag.rdd,GalaxyTag.schema).createOrReplaceTempView("GalaxyTag");
    GalaxyTag.write.format("parquet").save(PARENT_DIR + "data_parquet/GalaxyTag.parquet");

val star = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
     .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "star.csv")
    sparkSession.sqlContext.createDataFrame(star.rdd,star.schema).createOrReplaceTempView("star");
    star.write.format("parquet").save(PARENT_DIR + "data_parquet/star.parquet");

val propermotions = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
     .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "propermotions.csv")
    sparkSession.sqlContext.createDataFrame(propermotions.rdd,propermotions.schema).createOrReplaceTempView("propermotions");
    propermotions.write.format("parquet").save(PARENT_DIR + "data_parquet/propermotions.parquet");


//val xxxxxxxx = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
   //  .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "xxxxxxxxxxxxxxx.csv")
   // sparkSession.sqlContext.createDataFrame(xxxxxxxx.rdd,xxxxxxxx.schema).createOrReplaceTempView("xxxxxxxx");
   // xxxxxxxx.write.format("parquet").save(PARENT_DIR + "data_parquet/xxxxxxxx.parquet");



  }
}
