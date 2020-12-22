package definition

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, NamedExpression}
import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{LogicalRDD, UnaryExecNode}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import sketch.Sketch

import scala.collection.{Seq, mutable}

object  Paths {
  //val parentDir = "/home/hamid/TASTER/"
    var parentDir="/home/hamid/QAL/"
  var pathToTableCSV=parentDir+"data_csv/"
  var pathToSketches=parentDir+"materializedSketches/"
  var pathToQueryLog=parentDir+"queryLog"
  var pathToTableParquet=parentDir+"data_parquet/"
  var pathToSaveSynopses = parentDir + "materializedSynopsis/"
  var pathToCIStats = parentDir + "CIstats/"
  val tableName: mutable.HashMap[String, String] = new mutable.HashMap()
  val seed = 5427500315423L
  var REST=false
  val sketchesMaterialized=new mutable.HashMap[String,Sketch]()
  var counterForQueryRow=0
  var outputOfQuery=""
  val delimiterSynopsesColumnName = "@"
  val delimiterSynopsisFileNameAtt = ";"
  val delimiterParquetColumn = ","
  val startSamplingRate = 5
  val stopSamplingRate = 50
  val samplingStep = 5
  var LRUorWindowBased:Boolean=false
  var lastUsedCounter:Long=0
  val testFraction= Array(0.1)
  val testWindowSize=Array(50)
  var maxSpace = 1000
  var windowSize =5
  var fraction = 1.0
  val start =  0
  val testSize =350
  var numberOfSynopsesReuse=0
  var numberOfGeneratedSynopses=0
  val minNumOfOcc = 15
  var counterNumberOfRowGenerated=0
  var timeForUpdateWarehouse:Long=0
  var timeForSampleConstruction:Long=0
  var timeTotal:Long=0
  var timeForSubQueryExecution:Long=0
  var timeForTokenizing:Long=0
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
  val lastUsedOfParquetSample: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()
  val tableToCount:mutable.HashMap[String,Long]=new mutable.HashMap[String,Long]()
  val warehouseParquetNameToSize=new mutable.HashMap[String,Long]()
  val ParquetNameToSynopses = mutable.HashMap[String, String]()
  val SynopsesToParquetName = mutable.HashMap[String, String]()
  val parquetNameToHeader=new mutable.HashMap[String,String]()
  var JDBCRowLimiter:Long=100000000
  def getSizeOfAtt(in:Seq[Attribute])=in.map(x=>x.dataType.defaultSize).reduce(_+_)
  def getHeaderOfOutput(output: Seq[Attribute]): String =
    output.map(o => tableName.get(o.toString().toLowerCase).get + "." + o.name.split("#")(0).toLowerCase).mkString(delimiterParquetColumn)

  def getAttNameOfExpression(exps: Seq[NamedExpression]): Seq[String] =
    exps.map(o => {
      val att = o.find(_.isInstanceOf[AttributeReference]).map(_.asInstanceOf[AttributeReference]).get
      tableName.get(att.toString().toLowerCase).get + "." + att.name.split("#")(0).toLowerCase
    })

  def getAttRefOfExps(exps: Seq[NamedExpression]): Seq[AttributeReference] =
    exps.map(_.find(_.isInstanceOf[AttributeReference]).map(_.asInstanceOf[AttributeReference]).get)

  def getAttOfExpression(exps: Seq[NamedExpression]): Seq[AttributeReference] =
    exps.map(o => {
      o.find(_.isInstanceOf[AttributeReference]).map(_.asInstanceOf[AttributeReference]).get
    })

  def getTableColumnsName(output: Seq[Attribute]): Seq[String] =
    output.map(o => tableName.get(o.toString().toLowerCase).get.split("\\.")(0).dropRight(2) + "." + o.name.split("#")(0).toLowerCase)

  def getAttNameOfJoinKey(joinKeys: Seq[AttributeReference]): Seq[String] =
    joinKeys.map(o => tableName.get(o.toString().toLowerCase).get + "." + o.name.split("#")(0).toLowerCase)

  def getAttNameOfAtt(att: Attribute): String =
    tableName.get(att.toString().toLowerCase).get + "." + att.name.split("#")(0).toLowerCase


  def getEqualToFromExpression(exp: Expression): Seq[org.apache.spark.sql.catalyst.expressions.EqualTo] = exp match {
    case a: UnaryExpression =>
      Seq()
    case b@org.apache.spark.sql.catalyst.expressions.EqualTo(left, right) =>
      if (left.isInstanceOf[AttributeReference] && right.isInstanceOf[AttributeReference])
        Seq(b)
      else
        Seq()
    case b: BinaryExpression =>
      getEqualToFromExpression(b.left) ++ getEqualToFromExpression(b.right)
    case _ =>
      Seq()
  }

  def checkJoin(lp:LogicalPlan):Boolean= lp match {
    case j: Join =>
      true
    case l:LogicalRDD=>
      false
    case n =>
      n.children.foreach(child => if (checkJoin(child)) return true)
      false
  }


  /*  def getFunctionsName(funcs:Seq[AggregateExpression]):Seq[String]={
    funcs.map(x=>x.)
  }*/
}
