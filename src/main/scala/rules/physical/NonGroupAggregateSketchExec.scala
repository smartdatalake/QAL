package rules.physical

import definition.Paths.getScalingFactor
import operators.physical._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Count, Sum}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, BinaryComparison, Cast, NamedExpression, SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.{LogicalRDD, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}

import scala.collection.Seq
import scala.collection.mutable.ListBuffer
import org.apache.spark.unsafe.types.UTF8String

case class GroupAggregateSketchExec(confidence: Double, error: Double, seed: Long, groupingExpressions: Seq[NamedExpression], resultExpressions: Seq[NamedExpression]
                                    , functionsWithoutDistinct: Seq[AggregateExpression], hyperRects: Seq[Seq[And]], logicalScan: LogicalRDD)
  extends AggreagateSketchExec(confidence, error, seed, resultExpressions, functionsWithoutDistinct, hyperRects, logicalScan) {
  var countChildIndex = 0
  val edges = groupingExpressions.map(_.asInstanceOf[AttributeReference])

  override protected def doExecute(): RDD[InternalRow] = {

    val output = new ListBuffer[UnsafeRow]
    val outputSchema = resultExpressions.map(_.dataType).toArray
    var result = Array.fill[Array[InternalRow]](children.size)(null)
    val childExecuteSize = if (hasCount == true && children.size > 1) children.size - 2 else children.size - 1
    //todo multiple group by key
    //todo row update to unsafe
    for (i <- 0 to children.size - 1)
      result(i) = children(i).execute().collect()
    for (i <- 0 to result(0).size - 1) {
      val x = UnsafeProjection.create(outputSchema)
      val row = new SpecificInternalRow(outputSchema)
      row.update(0, result(0)(i).get(0, outputSchema(0)))
      for (j <- 0 to childExecuteSize)
        if (outputSchema(j + 1).isInstanceOf[IntegerType])
          row.setInt(j + 1, result(j)(i).getInt(1))
        else if (outputSchema(j + 1).isInstanceOf[LongType])
          row.setLong(j + 1, result(j)(i).getLong(1))
        else if (outputSchema(j + 1).isInstanceOf[StringType])
          row.update(j + 1, UTF8String.fromString(result(j)(i).getLong(1).toString))
        else {
          val u = (result(j)(i).getDouble(1) / result(countChildIndex)(i).getDouble(1))
          row.setDouble(j + 1, BigDecimal(u).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble)
        }

      output += x(row)
    }


    //    if(resultExpressions(0).children(0).asInstanceOf[AttributeReference].name.contains("avg")) {
    //    val count= childern.last.execute().collect()(0).getInt(0)
    //     SparkContext.getOrCreate().parallelize(Seq(getUnsafeRow(resultExpressions(0), result(0)/count)))
    // }
    // else
    SparkContext.getOrCreate().parallelize(output)
  }

  def getGroupingAttEdgeIndex(att: NamedExpression): Int = {
    0
  }

  override def children: Seq[SparkPlan] = {
    val sketches = new ListBuffer[SparkPlan]
    if (hyperRects == null || IsPointQuery) {
      for (func <- functionsWithoutDistinct) {
        val p = if (func.aggregateFunction.children(0).isInstanceOf[Cast]) func.aggregateFunction.children(0).children(0) else func.aggregateFunction.children(0)
        if (func.aggregateFunction.isInstanceOf[Sum])
          sketches += GroupCountMinSketchExec(p.asInstanceOf[AttributeReference]
            , groupingExpressions(0), hyperRects, edges(0), confidence, error, seed, func, logicalScan)
        else if (func.aggregateFunction.isInstanceOf[Average]) {
          sketches += GroupCountMinSketchExec(p.asInstanceOf[AttributeReference]
            , groupingExpressions(0), hyperRects, edges(0), confidence, error, seed, func, logicalScan)
          countChildIndex = -1
        }
        else
          sketches += GroupCountMinSketchExec(null, groupingExpressions(0), hyperRects, edges(0), confidence, error, seed, func, logicalScan)
      }
      if (countChildIndex == -1) {
        if (hasCount)
          for (i <- 0 to sketches.size - 1) {
            if (sketches(i).asInstanceOf[GroupByMultiDyadicRangeExec].resultExpression.aggregateFunction.isInstanceOf[Count])
              countChildIndex = i
          }
        else {
          sketches += GroupCountMinSketchExec(null, groupingExpressions(0), hyperRects, edges(0), confidence, error, seed
            , functionsWithoutDistinct(0), logicalScan)
          countChildIndex = sketches.size - 1
        }
      }
    } else {
      for (func <- functionsWithoutDistinct)
        if (func.aggregateFunction.isInstanceOf[Sum])
          sketches += GroupByMultiDyadicRangeExec(func.aggregateFunction.children(0).children(0).asInstanceOf[AttributeReference]
            , groupingExpressions(0), confidence, error, seed, func, hyperRects, edges, logicalScan)
        else if (func.aggregateFunction.isInstanceOf[Average]) {
          sketches += GroupByMultiDyadicRangeExec(func.aggregateFunction.children(0).children(0).asInstanceOf[AttributeReference]
            , groupingExpressions(0), confidence, error, seed, func, hyperRects, edges, logicalScan)
          countChildIndex = -1
        }
        else
          sketches += GroupByMultiDyadicRangeExec(null, groupingExpressions(0), confidence, error, seed
            , func, hyperRects, edges, logicalScan)
      if (countChildIndex == -1) {
        if (hasCount)
          for (i <- 0 to sketches.size - 1) {
            if (sketches(i).asInstanceOf[GroupByMultiDyadicRangeExec].resultExpression.aggregateFunction.isInstanceOf[Count])
              countChildIndex = i
          }
        else {
          sketches += GroupByMultiDyadicRangeExec(null, groupingExpressions(0), confidence, error, seed
            , functionsWithoutDistinct(1), hyperRects, edges, logicalScan)
          countChildIndex = sketches.size - 1
        }
      }
    }
    sketches
  }
}

case class NonGroupAggregateSketchExec(confidence: Double, error: Double, seed: Long, resultExpressions: Seq[NamedExpression]
                                       , functionsWithoutDistinct: Seq[AggregateExpression], hyperRects: Seq[Seq[And]], logicalScan: LogicalRDD)
  extends AggreagateSketchExec(confidence, error, seed, resultExpressions, functionsWithoutDistinct, hyperRects, logicalScan) {
  val edges = getDimension(hyperRects)
  /*    for (aggExpression <- functionsWithoutDistinct) {
      if (aggExpression.aggregateFunction.isInstanceOf[Count]) {
        var sketchLogicalRDD = plan.children(0)
        val sketchResults = resultExpressions
        var sketchProjectAtt: NamedExpression = null
        // todo what if no project
        while (!sketchLogicalRDD.isInstanceOf[LeafNode] && sketchLogicalRDD != null) {
          //todo count(*)
          if (sketchLogicalRDD.isInstanceOf[Project]) {
            sketchProjectAtt = sketchLogicalRDD.asInstanceOf[Project].projectList.filter(x
            => x.exprId == aggExpression.aggregateFunction.children(0).asInstanceOf[AttributeReference].exprId)(0)
          }
          sketchLogicalRDD = sketchLogicalRDD.children(0)
        }
        //val possibleSketches = getPossibleSketch (.9, 0.0001, 9223372036854775783L, sketchResults, sketchCondition
        //, sketchProjectAtt, sketchLogicalRDD.asInstanceOf[LogicalRDD] )
        val countMinSketchExec = null //CountMinSketchExec(1E-10, 0.001, 9223372036854775783L, sketchResults, sketchCondition, sketchProjectAtt, sketchLogicalRDD.asInstanceOf[LogicalRDD])


      }

    }*/

  // override def inputRDDs(): Seq[RDD[InternalRow]] = childern.flatMap(x=>x.asInstanceOf[CodegenSupport].inputRDDs())

  //TODO what is doProduce???
  //  override protected def doProduce(ctx: CodegenContext): String = childern.flatMap(x=>x.asInstanceOf[CodegenSupport].produce(ctx, this)).reduce(_.toString+_.toString)

  override protected def doExecute(): RDD[InternalRow] = {
    val output = new ListBuffer[UnsafeRow]
    var childIndex = 0
    var outputIndex = 0
    val outputSchema = resultExpressions.map(_.dataType).toArray
    var result = Array.fill[Array[InternalRow]](children.size)(null)
    for (i <- 0 to children.size - 1) {
      result(i) = children(i).execute().collect()
    }
    val x = UnsafeProjection.create(outputSchema)
    val row = new SpecificInternalRow(outputSchema)
    for (resultExp <- resultExpressions) {
      if ((resultExp.children(0).isInstanceOf[Cast] && resultExp.children(0).children(0).asInstanceOf[AttributeReference].name.contains("avg"))
        || (resultExp.children(0).isInstanceOf[AttributeReference] && resultExp.children(0).asInstanceOf[AttributeReference].name.contains("avg"))) {
        val u = (result(childIndex)(0).getDouble(1) / result(childIndex + 1)(0).getDouble(1))
        row.update(outputIndex, UTF8String.fromString(BigDecimal(u).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble.toString))
        outputIndex += 1
        childIndex += 2
      }
      else {
        if (outputSchema(outputIndex).isInstanceOf[IntegerType])
          row.update(outputIndex, result(childIndex)(0).getInt(1))
        else if (outputSchema(outputIndex).isInstanceOf[LongType])
          row.update(outputIndex, result(childIndex)(0).getLong(1))
        else if (outputSchema(outputIndex).isInstanceOf[StringType]) {
          val x = result(0)(0).getInt(1)
          row.update(outputIndex, UTF8String.fromString(result(childIndex)(0).getLong(1).toString()))
        } else
          row.update(outputIndex, result(childIndex)(0).getDouble(1))
        outputIndex += 1
        childIndex += 1
      }
    }
    output += x(row)


    //    if(resultExpressions(0).children(0).asInstanceOf[AttributeReference].name.contains("avg")) {
    //    val count= childern.last.execute().collect()(0).getInt(0)
    //     SparkContext.getOrCreate().parallelize(Seq(getUnsafeRow(resultExpressions(0), result(0)/count)))
    // }
    // else
    SparkContext.getOrCreate().parallelize(output)

  }

  /*    private def toUnsafeRow(row: Seq[Int], schema: Array[DataType]): UnsafeRow = {
      val converter = unsafeRowConverter(schema)
      converter(row)
    }

    private def unsafeRowConverter(schema: Array[DataType]): Seq[Int] => UnsafeRow = {
      val converter = UnsafeProjection.create(schema)
ProjectExec
      (row: Seq[Int]) => {
        converter(CatalystTypeConverters.convertToCatalyst(row))
      }
    }*/
  def children: Seq[SparkPlan] = {
    val sketches = new ListBuffer[DyadicRangeExec]
    for (func <- functionsWithoutDistinct)
      if (func.aggregateFunction.children(0).isInstanceOf[Cast]) {
        if (func.aggregateFunction.isInstanceOf[Sum])
          sketches += DyadicRangeExec(func.aggregateFunction.children(0).children(0).asInstanceOf[AttributeReference], confidence, error, seed, func, hyperRects(0), edges(0), logicalScan)
        else if (func.aggregateFunction.isInstanceOf[Average]) {
          sketches += DyadicRangeExec(func.aggregateFunction.children(0).children(0).asInstanceOf[AttributeReference], confidence, error, seed, func, hyperRects(0), edges(0), logicalScan)
          sketches += DyadicRangeExec(null, confidence, error, seed, func, hyperRects(0), edges(0), logicalScan)
        } else
          sketches += DyadicRangeExec(null, confidence, error, seed, func, hyperRects(0), edges(0), logicalScan)
      }
      else {
        if (func.aggregateFunction.isInstanceOf[Sum])
          sketches += DyadicRangeExec(func.aggregateFunction.children(0).asInstanceOf[AttributeReference], confidence, error, seed, func, hyperRects(0), edges(0), logicalScan)
        else if (func.aggregateFunction.isInstanceOf[Average]) {
          sketches += DyadicRangeExec(func.aggregateFunction.children(0).asInstanceOf[AttributeReference], confidence, error, seed, func, hyperRects(0), edges(0), logicalScan)
          sketches += DyadicRangeExec(null, confidence, error, seed, func, hyperRects(0), edges(0), logicalScan)
        }
        else
          sketches += DyadicRangeExec(null, confidence, error, seed, func, hyperRects(0), edges(0), logicalScan)
      }
    sketches
  }
}

abstract class AggreagateSketchExec(confidence: Double, error: Double, seed: Long, resultExpressions: Seq[NamedExpression]
                                    , functionsWithoutDistinct: Seq[AggregateExpression], hyperRects: Seq[Seq[And]]
                                    , logicalScan: LogicalRDD) extends MultiExecNode {

  //todo fix
  val IsPointQuery = true //if(hyperRects != null) hyperRects.forall(x=>x.forall(y=>y.left.asInstanceOf[BinaryComparison].right.asInstanceOf[Literal].value
  //==y.right.asInstanceOf[BinaryComparison].right.asInstanceOf[Literal].value)) else false

  def getDimension(hyperRects: Seq[Seq[And]]): Seq[AttributeReference] = {
    val elem = new ListBuffer[AttributeReference]
    for (hyperRect <- hyperRects) {
      for (edge <- hyperRect) {
        if (elem.forall(_.exprId != edge.left.asInstanceOf[BinaryComparison].left.asInstanceOf[AttributeReference].exprId))
          elem += edge.left.asInstanceOf[BinaryComparison].left.asInstanceOf[AttributeReference]
      }
    }
    elem
  }

  var hasCount = false
  functionsWithoutDistinct.foreach(x =>
    if (x.aggregateFunction.isInstanceOf[Count]) hasCount = true)

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  def getUnsafeRow(exp: NamedExpression, value: Long): UnsafeRow = exp.dataType match {
    case StringType => {
      ExpressionEncoder[String].toRow(value.toString()) match {
        case ur: UnsafeRow => ur
      }
    }
    case LongType =>
      ExpressionEncoder[Long].toRow(value) match {
        case ur: UnsafeRow => ur
      }
    case DoubleType =>
      ExpressionEncoder[Double].toRow(value) match {
        case ur: UnsafeRow => ur
      }
    case _ =>
      null
  }
}

case class ScaleAggregateSampleExec(confidence: Double, error: Double, seed: Long, fraction: Double, resultsExpression: Seq[NamedExpression], child: SparkPlan)
  extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  // override def inputRDDs(): Seq[RDD[InternalRow]] = childern.flatMap(x=>x.asInstanceOf[CodegenSupport].inputRDDs())

  //TODO what is doProduce???
  //  override protected def doProduce(ctx: CodegenContext): String = childern.flatMap(x=>x.asInstanceOf[CodegenSupport].produce(ctx, this)).reduce(_.toString+_.toString)

  lazy val scalingFactor = getScalingFactor(child)

  //TODO is it ok with changing accuracy
  override protected def doExecute(): RDD[InternalRow] = {
    if (isScaled.contains(true))
      child.execute().map(scale)
    else
      child.execute()
  }

  val isScaled = resultsExpression.map(x => if (x.find(x => x.toString().contains("count(") || x.toString().contains("sum(")).isDefined) true else false).toSeq
  val types = resultsExpression.map(x => x.toAttribute.dataType)

  def scale(i: InternalRow): InternalRow = {
    val row = new SpecificInternalRow(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
    val x = UnsafeProjection.create(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
    println(scalingFactor)
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

    return x(row)
    row.update(1, "asd")
    // row.setDouble(0, percent)
    //    row.setInt(1, point)
    //    val x = UnsafeProjection.create(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
    //    if(percent<100)
    //       result += x(row)
    //    percent += (100 / quantilePart)
    //  }
    //   SparkContext.getOrCreate().parallelize(result)

    // for (index <- 0 to i.numFields - 1) {
    //   if ((!resultsExpression(index).isInstanceOf[AttributeReference]) && (resultsExpression(index).children(0).children(0).asInstanceOf[AttributeReference].name.contains("sum")
    //     || resultsExpression(index).children(0).children(0).asInstanceOf[AttributeReference].name.contains("count"))) {
    if (i.getString(1).toInt > 0) {
      i.update(1, (i.getString(1).toInt * (scalingFactor)).toInt)
      return i
      //      }
      //    }

    }
    i
  }
}





