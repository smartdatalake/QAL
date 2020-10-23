package definition

  object  Paths {
   val parentDir = "/home/hamid/TASTER/"
   //  val parentDir="/home/sdlhshah/spark-data/"
   val pathToSaveSynopses = parentDir + "materializedSynopsis/"
   val pathToSaveSchema = parentDir + "materializedSchema/"
   val pathToCIStats = parentDir + "CIstats/"

   val seed = 5427500315423L
   val windowSize = 5
   val pathToSynopsesFileName = parentDir + "SynopsesToFileName.txt"
   val pathToTableSize = parentDir + "tableSize.txt"
   val delimiterSynopsesColumnName = "#"
   val delimiterSynopsisFileNameAtt = ";"
   val delimiterParquetColumn = ","
   val startSamplingRate = 5
   val stopSamplingRate = 50
   val samplingStep = 5
   val maxSpace = 3000

   val costOfFilter: Long = 1
   val costOfProject: Long = 1
   val costOfScan: Long = 1
   val costOfJoin: Long = 1
   val filterRatio: Double = 0.9
   val costOfUniformSample: Long = 1
   val costOfUniformWithoutCISample: Long = 1
   val costOfUniversalSample: Long = 1
   val costOfDistinctSample: Long = 1
   val costOfScale: Long = 1
   val HashAggregate: Long = 1

  }
