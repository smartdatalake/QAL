package definition

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, NamedExpression}

import scala.collection.{Seq, mutable}

object  Paths {
  val parentDir = "/home/hamid/TASTER/"
  //  val parentDir="/home/sdlhshah/spark-data/"
  val pathToSaveSynopses = parentDir + "materializedSynopsis/"
  val pathToSaveSchema = parentDir + "materializedSchema/"
  val pathToCIStats = parentDir + "CIstats/"
  val tableName: mutable.HashMap[String, String] = new mutable.HashMap()
  val seed = 5427500315423L
  val windowSize = 5
  val pathToSynopsesFileName = parentDir + "SynopsesToFileName.txt"
  val pathToTableSize = parentDir + "tableSize.txt"
  val delimiterSynopsesColumnName = "@"
  val delimiterSynopsisFileNameAtt = ";"
  val delimiterParquetColumn = ","
  val startSamplingRate = 5
  val stopSamplingRate = 50
  val samplingStep = 5
  val maxSpace = 15000

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
  val SortAggregate: Long = 1

  def getHeaderOfOutput(output: Seq[Attribute]): String =
    output.map(o => tableName.get(o.toString().toLowerCase).get + "." + o.name.split("#")(0).toLowerCase).mkString(delimiterParquetColumn)

  def getAttNameOfExpression(exps: Seq[NamedExpression]): Seq[String] = {
    exps.map(o => {
      val att = o.find(_.isInstanceOf[AttributeReference]).map(_.asInstanceOf[AttributeReference]).get
      tableName.get(att.toString().toLowerCase).get + "." + att.name.split("#")(0).toLowerCase
    })
  }

  def getTableColumnsName(output:Seq[Attribute]):Seq[String]={
    output.map(o => tableName.get(o.toString().toLowerCase).get.split("\\.")(0).dropRight(2) + "." + o.name.split("#")(0).toLowerCase)
  }

  def getAttNameOfJoinKey(joinKeys: Seq[AttributeReference]): Seq[String] =
    joinKeys.map(o => tableName.get(o.toString().toLowerCase).get + "." + o.name.split("#")(0).toLowerCase)


  /*  def getFunctionsName(funcs:Seq[AggregateExpression]):Seq[String]={
    funcs.map(x=>x.)
  }*/
}
