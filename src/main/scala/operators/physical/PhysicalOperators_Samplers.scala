package operators.physical

import java.util.UUID

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{LeafExecNode, _}
import org.apache.spark.sql.types._

import scala.collection.{Seq, mutable}
import scala.util.Random
import definition.Paths._
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution.joins.SortMergeJoinExec

abstract class SampleExec(confidence: Double, error: Double, func: Seq[AggregateExpression], child: SparkPlan) extends UnaryExecNode with CodegenSupport {


  def getError = error

  def getConfidence = confidence

  var sampleSize: Long = 0

  val fraction = if (child.find(x=>x.isInstanceOf[SampleExec] || (x.isInstanceOf[RDDScanExec]&&x.nodeName.contains("sample"))).isDefined) 1.0 else setStaticFraction(confidence, error)
  val zValue = Array.fill[Double](100)(0.0)
  zValue(99) = 2.58
  zValue(95) = 1.96
  zValue(90) = 1.64

  val joins = if (getJoinKeys(child).mkString(delimiterParquetColumn) == "") "_" else getJoinKeys(child).mkString(delimiterParquetColumn)

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  def calFactor(pp: SparkPlan): Double = pp match {
    case s: SampleExec =>
      s.fraction * calFactor(pp.children(0))
    case l@RDDScanExec(output, rdd, name, outputPartitioning, outputOrdering) =>
      if (name.contains("sample"))
        ParquetNameToSynopses.get(name).get.split(delimiterSynopsisFileNameAtt)(4).toDouble
      else
        1.0
    case u: UnaryExecNode =>
      calFactor(pp.children(0))
    case l: LeafExecNode =>
      1.0
    case _ =>
      pp.children.map(calFactor).reduce((a, b) => (a + b) / 2)
  }

  override def toString(): String = Seq("AbstractSample", getHeaderOfOutput(output), confidence, error, fraction, func.toString(), joins).mkString(delimiterSynopsisFileNameAtt)

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning


  override def inputRDDs(): Seq[RDD[InternalRow]] = child.asInstanceOf[CodegenSupport].inputRDDs()

  protected override def doProduce(ctx: CodegenContext): String = child.asInstanceOf[CodegenSupport].produce(ctx, this)

  def getTargetColumnIndex(aggExp: AggregateExpression): Int = {
    if (aggExp.aggregateFunction.isInstanceOf[Count] || aggExp.aggregateFunction.children(0).isInstanceOf[Count])
      return -1
    for (i <- 0 to output.size)
      if (aggExp.aggregateFunction.children(0).isInstanceOf[Cast] && output(i).name == aggExp.aggregateFunction.children(0).children(0).asInstanceOf[AttributeReference].name
        || aggExp.aggregateFunction.children(0).isInstanceOf[AttributeReference] && output(i).name == aggExp.aggregateFunction.children(0).asInstanceOf[AttributeReference].name)
        return i
    throw new Exception("The target column is not in table attributes")
  }

  def CLTCal(targetColumn: Int, data: RDD[InternalRow]): (Double, Double, Double) = {
    var n = 0.0
    var Ex: Long = 0
    var Ex2: Long = 0
    var temp = 0
    //todo make it with mapPerPartition
    data.collect().foreach(x => {
      if (!x.isNullAt(targetColumn)) {
        n = n + 1
        temp = x.getInt(targetColumn)
        Ex += temp
        Ex2 += temp * temp
      }
    })
    //todo for large value it overflows
    if (Ex2 < 0)
      return (Ex / n, 0.001, n)
    (Ex / n, ((Ex2 - (Ex / n) * Ex) / (n - 1)) / n, n)
  }

  //todo set better p and m
  def hashString(string: String): Int = {
    val p = 53;
    val m = 1e9 + 9;
    var hash_value = 0;
    var p_pow = 1;
    for (c <- string) {
      hash_value = ((hash_value + (c + 1) * p_pow) % m).toInt;
      p_pow = ((p_pow * p) % m).toInt;
    }
    hash_value
  }

  def getJoinKeys(pp: SparkPlan): Seq[String] = pp match {
    case SortMergeJoinExec(l, r, c, d, left, right) =>
      (Seq(Seq(getAttNameOfAttWithoutTableCounter(l(0).find(_.isInstanceOf[AttributeReference]).get.asInstanceOf[Attribute]), getAttNameOfAttWithoutTableCounter(r(0).find(_.isInstanceOf[AttributeReference]).get.asInstanceOf[Attribute])).sortBy(_.toString).mkString("=")) ++ getJoinKeys(left) ++ getJoinKeys(right)).sortBy(_.toString)
    case s: RDDScanExec =>
      val n = s.toString()
      if (n.contains("sample"))
        return ParquetNameToSynopses.get(n.substring(5, 31)).get.split(delimiterSynopsisFileNameAtt).last.split(delimiterParquetColumn).filter(!_.equals("_"))
      Seq()
    case _ =>
      pp.children.flatMap(getJoinKeys)
  }


}

case class UniformSampleExec2WithoutCI(seed: Long, child: SparkPlan) extends SampleExec(0, 0, null, child) {

  override def toString() = Seq("UniformWithoutCI", getHeaderOfOutput(output), 0, 0, calFactor(this), sampleSize, "null", joins).mkString(delimiterSynopsisFileNameAtt)

  override protected def doExecute(): RDD[InternalRow] = {
    if (fraction >= 1.0)
      return child.execute()
    child.execute().sample(false, fraction, seed) /*.mapPartitionsWithIndexInternal { (index, iter) =>
        if(index<3)
          iter
        else
          Iterator()}*/
    // sampleSize = out.count()
    //saveAsCSV(out, toString())
    //  out
  }

}

case class UniformSampleExec2(functions: Seq[AggregateExpression], confidence: Double, error: Double, seed: Long,
                              child: SparkPlan) extends SampleExec(confidence, error, functions, child) {

  var seenPartition = 0

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  override def toString() = Seq("Uniform", (if(output.size!=0)getHeaderOfOutput(output) else tableName.get(child.asInstanceOf[ProjectExec].child.output(0).toString().toLowerCase).get.split("_")(0)), confidence, error, calFactor(this), sampleSize
    , if (functions == null) "null" else functions.mkString(delimiterSynopsesColumnName), joins).mkString(delimiterSynopsisFileNameAtt)

  protected override def doExecute(): RDD[InternalRow] = {
    if (fraction >= 1.0)
      return child.execute()
    //  println(fraction)
    //  println(child.execute().sample(false, fraction).count())
    val weightIndexs = output.zipWithIndex.filter(_._1.name.contains("ww___ww")).map(_._2)
    val r = scala.util.Random
    val weightIndex = output.size - 1
    r.setSeed(seed)
    child.execute().mapPartitionsWithIndex { (index, iter) => {
      iter.flatMap { row =>
        val newRand = r.nextDouble
        if (newRand < fraction) {
          for (weightIndex <- weightIndexs)
            row.setDouble(weightIndex, BigDecimal(row.getDouble(weightIndex) * (1.0 / fraction)).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble)
          List(row)
        } else
          List()
      }
    }
      //val weightIndex = output.size - 1
      //child.execute().sample(false, fraction).map(row => {
      //  row.setDouble(weightIndex, BigDecimal(row.getDouble(weightIndex) * (1.0 / fraction)).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble)
      //  row
      //})
      /*.mapPartitionsWithIndexInternal { (index, iter) =>
        if(index<3)
          iter
        else
          Iterator()}*/
      //todo multiple operator on sample
      //todo without Cast
      /*      var sampleErrorForTargetConfidence = 0.0
      var targetError = 0.0
      val (appMean, appVariance, sampleSize) = CLTCal(getTargetColumnIndex(functions(0)), out)
      this.sampleSize = sampleSize.toInt
      if (functions(0).aggregateFunction.isInstanceOf[Average]) {
        targetError = (1 + error) * appMean
        val appSD = math.pow(appVariance, 0.5)
        sampleErrorForTargetConfidence = appMean + zValue((confidence * 100).toInt) * appSD
      }
      else if (functions(0).aggregateFunction.isInstanceOf[Sum]) {
        //todo null value is counted!!!
        val dataSize = input.count()
        val appSum = (appMean * dataSize)
        val appSumVariance = appVariance * dataSize * dataSize
        targetError = (1 + error) * appSum
        val appSD = math.pow(appSumVariance, 0.5)
        sampleErrorForTargetConfidence = appSum + zValue((confidence * 100).toInt) * appSD
      }
      else if (functions(0).aggregateFunction.isInstanceOf[Count]) {
        //todo null value is counted!!!
        //todo appVarince is 0.0
        val dataSize = input.count()
        val appSum = appMean * dataSize
        val appSumVariance = appVariance * dataSize * dataSize
        targetError = (1 + error) * appSum
        val appSD = math.pow(appSumVariance, 0.5)
        sampleErrorForTargetConfidence = appSum + zValue((confidence * 100).toInt) * appSD
      }
      else
        throw new Exception("Operator is not approximatable")
      if (sampleErrorForTargetConfidence < targetError) {
        out.saveAsObjectFile(pathToSaveSynopses + this.toString())
        return out
      }

      //seenPartition += 1
      fraction += fractionStep*/
      //   }
      //  out
    }

  }
}

case class DistinctSampleExec2(functions: Seq[AggregateExpression], confidence: Double, error: Double, seed: Long,
                               groupingExpression: Seq[NamedExpression],
                               child: SparkPlan) extends SampleExec(confidence, error, functions, child: SparkPlan) {

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  override def toString(): String =
    Seq("Distinct", getHeaderOfOutput(output), confidence, error, calFactor(this), sampleSize, functions.mkString(delimiterSynopsesColumnName)
      , getAccessedColsOfExpressions(groupingExpression).mkString(delimiterSynopsesColumnName), joins).mkString(delimiterSynopsisFileNameAtt)

  protected override def doExecute(): RDD[InternalRow] = {
    if (fraction >= 1.0)
      return child.execute()
    val r = scala.util.Random
    r.setSeed(seed)
    val groupValues: Seq[(Int, DataType)] = getAttOfExpression(groupingExpression).sortBy(_.toString()).map(x => {
      var index = -1
      for (i <- 0 to child.output.size - 1)
        if (child.output(i).name.toLowerCase == x.find(_.isInstanceOf[AttributeReference]).get.asInstanceOf[AttributeReference].name.toLowerCase)
          index = i
      if (index == -1)
        throw new Exception("The grouping key is not in table columns!!!!")
      (index, x.dataType)
    })
    //todo null are counted
    // println(fraction)
    //  println(child.execute().count())
    val weightIndexs = output.zipWithIndex.filter(_._1.name.contains("ww___ww")).map(_._2)
    child.execute().mapPartitionsWithIndex { (index, iter) => {
      // println(index)
      val sketch: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
      iter.flatMap { row =>
        val GroupKey = groupValues.map(gv => row.get(gv._1, gv._2)).mkString(",")
        val curCount = sketch.getOrElse(GroupKey, 0)
        if (curCount > 0) {
          if (curCount < 2 * minNumOfOcc) {
            sketch.update(GroupKey, sketch.getOrElse(GroupKey, 0) + 1)
            List(row)
          } else {
            val newRand = r.nextDouble
            if (newRand < fraction) {
              for (weightIndex <- weightIndexs)
                row.setDouble(weightIndex, BigDecimal(row.getDouble(weightIndex) * (1.0 / fraction)).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble)
              List(row)
            } else
              List()
          }
        } else {
          sketch.put(GroupKey, 1)
          List(row)
        }
      }
    }
    }
    /*      val (appMean, appVariance, sampleSize) = CLTCal(getTargetColumnIndex(functions(0)), out)
      var sampleErrorForTargetConfidence = 0.0
      var targetError = 0.0
      this.sampleSize = out.count()
      println(dataSize)

      // this.fraction=this.sampleSize/dataSize
      if (functions(0).aggregateFunction.isInstanceOf[Average]) {
        targetError = (1 + error) * appMean
        val appSD = math.pow(appVariance, 0.5)
        sampleErrorForTargetConfidence = appMean + zValue((confidence * 100).toInt) * appSD
      }
      else if (functions(0).aggregateFunction.isInstanceOf[Sum]) {
        //todo null value is counted!!!
        val appSum = (appMean * dataSize)
        val appSumVariance = appVariance * dataSize * dataSize
        targetError = (1 + error) * appSum
        val appSD = math.pow(appSumVariance, 0.5)
        sampleErrorForTargetConfidence = appSum + zValue((confidence * 100).toInt) * appSD
      }
      else if (functions(0).aggregateFunction.isInstanceOf[Count]) {
        //todo null value is counted!!!
        //todo appVarince is 0.0
        val appSum = appMean * dataSize
        val appSumVariance = appVariance * dataSize * dataSize
        targetError = (1 + error) * appSum
        val appSD = math.pow(appSumVariance, 0.5)
        sampleErrorForTargetConfidence = appSum + zValue((confidence * 100).toInt) * appSD
      }
      else
        throw new Exception("Operator is not approximatable")


      if (sampleErrorForTargetConfidence <= targetError) {
        out.saveAsObjectFile(pathToSaveSynopses + this.toString())
        return out
      }
      fraction += fractionStep*/
    //  }
    //  out
  }

  def getAttOfExpression(exps: Seq[NamedExpression]): Seq[AttributeReference] =
    exps.map(o => {
      o.find(_.isInstanceOf[AttributeReference]).map(_.asInstanceOf[AttributeReference]).get
    })

}

case class UniversalSampleExec2(functions: Seq[AggregateExpression], confidence: Double, error: Double, seed: Long
                                , joinKey: Seq[AttributeReference], child: SparkPlan) extends SampleExec(confidence
  , error, functions, child: SparkPlan) {


  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  override def toString(): String = {
    if (functions == null)
      return Seq("Universal", getHeaderOfOutput(output), confidence, error, calFactor(this), "null"
        , getAccessedColsOfExpressions(joinKey).mkString(delimiterSynopsesColumnName), joins).mkString(delimiterSynopsisFileNameAtt)
    Seq("Universal", getHeaderOfOutput(output), confidence, error, calFactor(this), functions.mkString(delimiterSynopsesColumnName)
      , getAccessedColsOfExpressions(joinKey).mkString(delimiterSynopsesColumnName), joins).mkString(delimiterSynopsisFileNameAtt)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    if (fraction >= 1.0)
      return child.execute()
    val weightIndexs = output.zipWithIndex.filter(_._1.name.contains("ww___ww")).map(_._2)
    val rand = new Random(seed)
    val a = math.abs(rand.nextInt()) / 10000
    val b = math.abs(rand.nextInt()) / 1000
    val s = math.abs(rand.nextInt())
    val joinAttrs: Seq[(Int, DataType)] = joinKey.map(x => {
      var index = -1
      for (i <- 0 to child.output.size - 1)
        if (child.output(i).name.toLowerCase == x.name.toLowerCase)
          index = i
      if (index == -1)
        throw new Exception("The join key is not in table columns!!!!")
      (index, x.dataType)
    })
    //todo multiple join key
    //todo null are counted
    val weightIndex = output.size - 1
    child.execute().mapPartitionsWithIndex { (index, iter) =>
      iter.flatMap { row =>
        if (row.get(joinAttrs(0)._1, joinAttrs(0)._2) == null)
          List()
        else {
          val join = if (joinAttrs(0)._2.isInstanceOf[StringType]) hashString(row.get(joinAttrs(0)._1, joinAttrs(0)._2).toString)
          else hashString(row.get(joinAttrs(0)._1, LongType).toString)
          var t = ((join * a + b) % s) % 100
          //todo make faster
          t = if (t < 0) (t + 100) else t
          if (t < fraction * 100) {
            for (weightIndex <- weightIndexs)
              row.setDouble(weightIndex, BigDecimal(row.getDouble(weightIndex) * (1.0 / fraction)).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble)
            List(row)
          }
          else
            List()
        }
      }
    }
    /*      val (appMean, appVariance, sampleSize) = CLTCal(getTargetColumnIndex(functions(0)), out)
      var targetError = 0.0
      var sampleErrorForTargetConfidence = 0.0
      this.sampleSize = sampleSize.toInt

      if (functions(0).aggregateFunction.isInstanceOf[Average]) {
        targetError = (1 + error) * appMean
        val appSD = math.pow(appVariance, 0.5)
        sampleErrorForTargetConfidence = appMean + zValue((confidence * 100).toInt) * appSD
      }
      else if (functions(0).aggregateFunction.isInstanceOf[Sum]) {
        //todo null value is counted!!!
        val appSum = (appMean * dataSize)
        val appSumVariance = appVariance * dataSize * dataSize
        targetError = (1 + error) * appSum
        val appSD = math.pow(appSumVariance, 0.5)
        sampleErrorForTargetConfidence = appSum + zValue((confidence * 100).toInt) * appSD
      }
      else if (functions(0).aggregateFunction.isInstanceOf[Count]) {
        //todo null value is counted!!!
        //todo appVarince is 0.0
        val appSum = appMean * dataSize
        val appSumVariance = appVariance * dataSize * dataSize
        targetError = (1 + error) * appSum
        val appSD = math.pow(appSumVariance, 0.5)
        sampleErrorForTargetConfidence = appSum + zValue((confidence * 100).toInt) * appSD
      }
      else
        throw new Exception("Operator is not approximatable")
      if (sampleErrorForTargetConfidence < targetError) {
        out.saveAsObjectFile(pathToSaveSynopses + this.toString())
        //out.saveAsTextFile(path + this.toString())

        return out
      }
      //  fraction += fractionStep*/
    //    }
    //   out
  }

}

case class ScaleAggregateSampleExec(confidence: Double, error: Double, seed: Long, resultsExpression: Seq[NamedExpression], child: SparkPlan)
  extends UnaryExecNode {

  lazy val scalingFactor = getScalingFactor(child)
  val isScaled = resultsExpression.map(x => if (x.find(x => x.toString().contains("count(") || x.toString().contains("sum(")).isDefined) true else false)
  val types = resultsExpression.map(x => x.toAttribute.dataType)

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  override def output: Seq[Attribute] = child.output

  def getScalingFactor(pp: SparkPlan): Double = pp match {
    case s: SampleExec =>
      (1 / s.fraction) * getScalingFactor(s.child)
    case l@RDDScanExec(output, rdd, name, outputPartitioning, outputOrdering) =>
      if (name.contains("sample")) {
        //println(ParquetNameToSynopses)
        1 / ParquetNameToSynopses.get(name).get.split(delimiterSynopsisFileNameAtt)(4).toDouble
      } else
        1.0
    case u: UnaryExecNode =>
      getScalingFactor(pp.children(0))
    case _ =>
      pp.children.map(getScalingFactor).reduce((a, b) => (a + b) / 2)
  }

  //TODO what is doProduce???
  // override protected def doProduce(ctx: CodegenContext): String = childern.flatMap(x=>x.asInstanceOf[CodegenSupport].produce(ctx, this)).reduce(_.toString+_.toString)

  //TODO is it ok with changing accuracy
  override protected def doExecute(): RDD[InternalRow] = {
    // if (isScaled.contains(true))
    //    child.execute().map(scale)
    //  else
    child.execute()
  }

  def scale(i: InternalRow): InternalRow = {
    val row = new SpecificInternalRow(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
    val x = UnsafeProjection.create(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
    for (index <- 0 to i.numFields - 1) if (isScaled(index))
      types(index) match {
        case LongType =>
          row.update(index, (i.get(index, types(index)).toString.toLong * (scalingFactor)).toLong)
        case DoubleType =>
          row.update(index, (i.get(index, types(index)).toString.toDouble * (scalingFactor)).toDouble)
        case IntegerType =>
          row.update(index, (i.get(index, types(index)).toString.toInt * (scalingFactor)).toInt)
        case _ => throw new Exception("Invalid DataType for scaling")
      }
    else
      row.update(index, i.get(index, types(index)))
    x(row)
  }
}