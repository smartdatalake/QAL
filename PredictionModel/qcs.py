from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructType

spark = SparkSession .builder.config("spark.sql.debug.maxToStringFields", "1000").config("spark.driver.memory", "2g") .getOrCreate()

# sc = SparkContext('local', 'Spark SQL')
df = spark.read.parquet("/home/hamid/QAL/QP/skyServer/data_parquet/specobjall.parquet")
df.createOrReplaceTempView("SpecObjAll")
df = spark.read.parquet("/home/hamid/QAL/QP/skyServer/data_parquet/photoobjall.parquet")
df.createOrReplaceTempView("PhotoObjAll")

print(spark.sql("SELECT COUNT(p.objid) as CNT FROM PhotoObjAll AS p JOIN SpecObjAll s ON p.objID = s.bestObjID WHERE p.dec >= 52 and p.dec < 60")._jdf.queryExecution().analyzed())
# Get all csv files in the given directory.
# files_csv = dir.glob('**/*.csv') # Also get csv files in underlying folders.
#files_csv = table_dir.glob('*.csv')
#paths_csv = [path.as_posix() for path in files_csv]

#for i in paths_csv:
#    table_name = i.split("/")[-1].split(".")[0]

#    df = spark.read.options(header=True, delimiter=",").csv(i)
#    df.createTempView(table_name)

# qry_log = spark.read.options(header=True, delimiter=",").csv(log)
# qrys = qry_log.select("statement").collect()

# for qry in qrys:
#     print(qry.statement)

#test_analyzed = spark.sql("SELECT COUNT(p.objid) as CNT FROM PhotoObjAll AS p JOIN SpecObjAll s ON p.objID = s.bestObjID WHERE p.dec >= 52 and p.dec < 60")._jdf.queryExecution().analyzed().toString()

#print(test_analyzed)