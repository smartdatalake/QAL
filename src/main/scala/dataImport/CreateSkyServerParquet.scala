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


    val sf = 1 //args(1);

    // hdfs or local
    val hdfsOrLocal = "local" //args(2);

    val format = "csv";

    var PARENT_DIR = ""
    if (hdfsOrLocal.equals("local")) {
      PARENT_DIR = "/home/hamid/TASTER/spark-data/skyServer/sf" + sf + "/";
    } else if (hdfsOrLocal.equals("hdfs")) {
      PARENT_DIR = "hdfs://145.100.59.58:9000/skyServer/sf" + sf + "/";
    }

    val DATA_DIR = PARENT_DIR + "data_" + format + "/";

    val specObjAll = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "specobjall.csv")
    sparkSession.sqlContext.createDataFrame(specObjAll.rdd, specObjAll.schema).createOrReplaceTempView("specobjall");
    specObjAll.write.format("parquet").save(PARENT_DIR + "data_parquet/specobjall.parquet");
    val plateX = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "plateX.csv")
    sparkSession.sqlContext.createDataFrame(plateX.rdd, plateX.schema).createOrReplaceTempView("plateX");
    plateX.write.format("parquet").save(PARENT_DIR + "data_parquet/plateX.parquet");
    val specPhotoAll = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "specphotoall.csv")
    sparkSession.sqlContext.createDataFrame(specPhotoAll.rdd, specPhotoAll.schema).createOrReplaceTempView("specphotoall");
    specPhotoAll.write.format("parquet").save(PARENT_DIR + "data_parquet/specphotoall.parquet");
    val sppParams = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "sppparams.csv")
    sparkSession.sqlContext.createDataFrame(sppParams.rdd, sppParams.schema).createOrReplaceTempView("sppparams");
    sppParams.write.format("parquet").save(PARENT_DIR + "data_parquet/sppparams.parquet");
    val SpecObj = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "specobj.csv")
    sparkSession.sqlContext.createDataFrame(SpecObj.rdd, SpecObj.schema).createOrReplaceTempView("specobj");
    SpecObj.write.format("parquet").save(PARENT_DIR + "data_parquet/specobj.parquet");
    val PhotoPrimary = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "photoprimary.csv")
    sparkSession.sqlContext.createDataFrame(PhotoPrimary.rdd, PhotoPrimary.schema).createOrReplaceTempView("photoprimary");
    PhotoPrimary.write.format("parquet").save(PARENT_DIR + "data_parquet/photoprimary.parquet");
    val specphoto = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "specphoto.csv")
    sparkSession.sqlContext.createDataFrame(specphoto.rdd, specphoto.schema).createOrReplaceTempView("specphoto");
    specphoto.write.format("parquet").save(PARENT_DIR + "data_parquet/specphoto.parquet");
    val photoobj = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "photoobj.csv")
    sparkSession.sqlContext.createDataFrame(photoobj.rdd, photoobj.schema).createOrReplaceTempView("photoobj");
    photoobj.write.format("parquet").save(PARENT_DIR + "data_parquet/photoobj.parquet");
    val wise_xmatch = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "wise_xmatch.csv")
    sparkSession.sqlContext.createDataFrame(wise_xmatch.rdd, wise_xmatch.schema).createOrReplaceTempView("wise_xmatch");
    wise_xmatch.write.format("parquet").save(PARENT_DIR + "data_parquet/wise_xmatch.parquet");
    val PhotoObjAll = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "photoobjall.csv")
    sparkSession.sqlContext.createDataFrame(PhotoObjAll.rdd, PhotoObjAll.schema).createOrReplaceTempView("photoobjall");
    PhotoObjAll.write.format("parquet").save(PARENT_DIR + "data_parquet/photoobjall.parquet");
    val emissionLinesPort = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "emissionlinesport.csv")
    sparkSession.sqlContext.createDataFrame(emissionLinesPort.rdd, emissionLinesPort.schema).createOrReplaceTempView("emissionlinesport");
    emissionLinesPort.write.format("parquet").save(PARENT_DIR + "data_parquet/emissionlinesport.parquet");
    val wise_allsky = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "wise_allsky.csv")
    sparkSession.sqlContext.createDataFrame(wise_allsky.rdd, wise_allsky.schema).createOrReplaceTempView("wise_allsky");
    wise_allsky.write.format("parquet").save(PARENT_DIR + "data_parquet/wise_allsky.parquet");
    val galSpecLine = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "galspecline.csv")
    sparkSession.sqlContext.createDataFrame(galSpecLine.rdd, galSpecLine.schema).createOrReplaceTempView("galspecline");
    galSpecLine.write.format("parquet").save(PARENT_DIR + "data_parquet/galspecline.parquet");
    val zooSPec = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "zoospec.csv")
    sparkSession.sqlContext.createDataFrame(zooSPec.rdd, zooSPec.schema).createOrReplaceTempView("zoospec");
    zooSPec.write.format("parquet").save(PARENT_DIR + "data_parquet/zoospec.parquet");
    val galaxy = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "galaxy.csv")
    sparkSession.sqlContext.createDataFrame(galaxy.rdd, galaxy.schema).createOrReplaceTempView("galaxy");
    galaxy.write.format("parquet").save(PARENT_DIR + "data_parquet/galaxy.parquet");
    val Photoz = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "photoz.csv")
    sparkSession.sqlContext.createDataFrame(Photoz.rdd, Photoz.schema).createOrReplaceTempView("photoz");
    Photoz.write.format("parquet").save(PARENT_DIR + "data_parquet/photoz.parquet");
    val zooNoSpec = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "zoonospec.csv")
    sparkSession.sqlContext.createDataFrame(zooNoSpec.rdd, zooNoSpec.schema).createOrReplaceTempView("zoonospec");
    zooNoSpec.write.format("parquet").save(PARENT_DIR + "data_parquet/zoonospec.parquet");
    val GalaxyTag = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "galaxytag.csv")
    sparkSession.sqlContext.createDataFrame(GalaxyTag.rdd, GalaxyTag.schema).createOrReplaceTempView("galaxyag");
    GalaxyTag.write.format("parquet").save(PARENT_DIR + "data_parquet/galaxytag.parquet");
    val FIRST = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "first.csv")
    sparkSession.sqlContext.createDataFrame(FIRST.rdd, FIRST.schema).createOrReplaceTempView("first");
    FIRST.write.format("parquet").save(PARENT_DIR + "data_parquet/first.parquet");
    val Field = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "field.csv")
    sparkSession.sqlContext.createDataFrame(Field.rdd, Field.schema).createOrReplaceTempView("field");
    Field.write.format("parquet").save(PARENT_DIR + "data_parquet/field.parquet");
    val star = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "star.csv")
    sparkSession.sqlContext.createDataFrame(star.rdd, star.schema).createOrReplaceTempView("star");
    star.write.format("parquet").save(PARENT_DIR + "data_parquet/star.parquet");
    val propermotions = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "propermotions.csv")
    sparkSession.sqlContext.createDataFrame(propermotions.rdd, propermotions.schema).createOrReplaceTempView("propermotions");
    propermotions.write.format("parquet").save(PARENT_DIR + "data_parquet/propermotions.parquet");
    val stellarmassstarformingport = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "stellarmassstarformingport.csv")
    sparkSession.sqlContext.createDataFrame(stellarmassstarformingport.rdd, stellarmassstarformingport.schema).createOrReplaceTempView("stellarmassstarformingport");
    stellarmassstarformingport.write.format("parquet").save(PARENT_DIR + "data_parquet/stellarmassstarformingport.parquet");
    val sdssebossfirefly = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "sdssebossfirefly.csv")
    sparkSession.sqlContext.createDataFrame(sdssebossfirefly.rdd, sdssebossfirefly.schema).createOrReplaceTempView("sdssebossfirefly");
    sdssebossfirefly.write.format("parquet").save(PARENT_DIR + "data_parquet/sdssebossfirefly.parquet");
    val spplines = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "spplines.csv")
    sparkSession.sqlContext.createDataFrame(spplines.rdd, spplines.schema).createOrReplaceTempView("spplines");
    spplines.write.format("parquet").save(PARENT_DIR + "data_parquet/spplines.parquet");


    //val xxxxxxxx = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
    //  .option("inferSchema", "true").option("delimiter", ",").option("nullValue", "null").load(DATA_DIR + "xxxxxxxxxxxxxxx.csv")
    // sparkSession.sqlContext.createDataFrame(xxxxxxxx.rdd,xxxxxxxx.schema).createOrReplaceTempView("xxxxxxxx");
    // xxxxxxxx.write.format("parquet").save(PARENT_DIR + "data_parquet/xxxxxxxx.parquet");

  }
}
