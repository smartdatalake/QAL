package operators.physical

import definition.Paths.sketchesMaterialized
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.execution.{BinaryExecNode, RDDScanExec, SparkPlan, UnaryExecNode}
import sketch.DyadicRanges

import scala.collection.Seq
import scala.collection.mutable.ListBuffer

case class QuantileSampleExec(quantileColAtt: AttributeReference, quantilePart: Int, output: Seq[Attribute], child: SparkPlan) extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = {
    val result = new ListBuffer[UnsafeRow]
    val index=child.output.zipWithIndex.find(_._1.name.equalsIgnoreCase(quantileColAtt.name)).get._2
    val list = child.execute.filter(_.get(index, child.output(index).dataType) != null).map(x => x.get(index, child.output(index).dataType).toString.toDouble).sortBy(x => x).collect().toList
    var percent = 100 / quantilePart.toDouble
    if (list.size == 0) {
      val row = new SpecificInternalRow(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
      row.setDouble(0, 0)
      row.setDouble(1, 0)
      val x = UnsafeProjection.create(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
      result += x(row)
      return SparkContext.getOrCreate().parallelize(result)
    }
    for (i <- 0 until list.size by list.size / quantilePart) {
      val row = new SpecificInternalRow(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
      row.setDouble(0, percent)
      row.setDouble(1, list(i))
      val x = UnsafeProjection.create(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
      if (percent < 100)
        result += x(row)
      percent += (100 / quantilePart)
    }
    SparkContext.getOrCreate().parallelize(result)
  }
}

case class BinningWithoutMaxMinSampleExec(binningPart: Int, output: Seq[Attribute], child: DyadicRangeExec)
  extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = ???

}

case class BinningSampleExec(binningPart: Int, binningStart: Double, binningEnd: Double, output: Seq[Attribute], child: DyadicRangeExec)
  extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = {
    val result = new ListBuffer[UnsafeRow]
    val bucketSize = (binningStart - binningEnd) / binningPart.toDouble
    val list = child.execute.map(x => x.get(0, child.output(0).dataType).toString.toDouble).map(x => {
      var part = binningPart - 1
      for (i <- 0 to binningPart - 1)
        if (i * bucketSize <= x && x < (i + 1) * bucketSize)
          part = i
      (part, 1)
    }).reduceByKey(_ + _).sortBy(x => x._1).collect().toList
    if (list.size == 0) {
      for (i <- 0 to binningPart - 1) {
        val row = new SpecificInternalRow(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
        row.setDouble(0, i * bucketSize + binningStart)
        row.setDouble(1, (i + 1) * bucketSize + binningStart)
        row.setInt(2, 0)
        val x = UnsafeProjection.create(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
        result += x(row)
      }
      return SparkContext.getOrCreate().parallelize(result)
    }
    for (i <- 0 to binningPart - 1) {
      val row = new SpecificInternalRow(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
      row.setDouble(0, i * bucketSize + binningStart)
      row.setDouble(1, (i + 1) * bucketSize + binningStart)
      row.setInt(2, list(i)._2)
      val x = UnsafeProjection.create(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
      result += x(row)
    }
    SparkContext.getOrCreate().parallelize(result)
  }

}

case class QuantileSketchExec(quantilePart: Int, output: Seq[Attribute], child: DyadicRangeExec) extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = {
    val dr = sketchesMaterialized.get(child.toString).get.asInstanceOf[DyadicRanges]
    val result = new ListBuffer[UnsafeRow]
    val points = dr.getQuantiles(quantilePart)
    var percent = 100 / quantilePart.toDouble
    for (point <- points) {
      val row = new SpecificInternalRow(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
      row.setDouble(0, percent)
      row.setDouble(1, point.toDouble)
      val x = UnsafeProjection.create(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
      result += x(row)
      percent += (100 / quantilePart)
    }
    SparkContext.getOrCreate().parallelize(result)
  }

}

case class BinningSketchExec(binningPart: Int, binningStart: Double, binningEnd: Double, output: Seq[Attribute], child: DyadicRangeExec)
  extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = {
    val dr = sketchesMaterialized.get(child.toString).get.asInstanceOf[DyadicRanges]
    val result = new ListBuffer[UnsafeRow]
    val bucketSize = (binningEnd - binningStart) / binningPart
    for (i <- 0 to binningPart - 1) {
      val row = new SpecificInternalRow(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
      row.setDouble(0, i * bucketSize + binningStart)
      row.setDouble(1, (i + 1) * bucketSize + binningStart)
      row.setInt(2, dr.get((i * bucketSize + binningStart).toInt, ((i + 1) * bucketSize + binningStart).toInt))
      val x = UnsafeProjection.create(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
      result += x(row)
    }
    SparkContext.getOrCreate().parallelize(result)
  }

}

case class BinningWithoutMaxMinSketchExec(binningPart: Int, output: Seq[Attribute], child: DyadicRangeExec)
  extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = {
    val dr = sketchesMaterialized.get(child.toString).get.asInstanceOf[DyadicRanges]
    val result = new ListBuffer[UnsafeRow]
    val bucketSize = (dr.getMax - dr.getMin) / binningPart
    for (i <- 0 to binningPart - 1) {
      val row = new SpecificInternalRow(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
      row.setDouble(0, i * bucketSize + dr.getMin)
      row.setDouble(1, (i + 1) * bucketSize + dr.getMin)
      row.setInt(2, dr.get((i * bucketSize + dr.getMin).toInt, ((i + 1) * bucketSize + dr.getMin).toInt))
      val x = UnsafeProjection.create(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
      result += x(row)
    }
    SparkContext.getOrCreate().parallelize(result)
  }

}

case class WeightCombiner(child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = if (child.output.count(_.name.equalsIgnoreCase("ww___ww")) == 2)
    child.output.filterNot(_.name.equalsIgnoreCase("ww___ww")) ++ Seq(child.output.last)
  else
    child.output

  override def doExecute(): RDD[InternalRow] = {
    if (output.size == child.output.size)
      child.execute()
    else if (child.children(1).find(x => x.isInstanceOf[SampleExec] || (x.isInstanceOf[RDDScanExec] && x.toString().contains("sample"))).isDefined)
      child.execute().mapPartitionsWithIndex { (index, iter) =>
        val project = UnsafeProjection.create(output, child.output, true)
        project.initialize(index)
        iter.map(project)
      }
    else  if (child.children(0).find(x => x.isInstanceOf[SampleExec] || (x.isInstanceOf[RDDScanExec] && x.toString().contains("sample"))).isDefined){
      val mainIndex = child.output.size - 1
      val secondIndex = child.children(0).output.size - 1
      child.execute().mapPartitionsWithIndex { (index, iter) =>
        val project = UnsafeProjection.create(output, child.output, true)
        project.initialize(index)
        iter.map(row => {
          row.setDouble(mainIndex, row.getDouble(secondIndex))
          row
        }).map(project)
      }
    }
    else
      throw new Exception("Invalid input for weightCombiner.")
  }

}


case class XXX(child: SparkPlan, aggCol: Seq[AttributeReference]) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override protected def doExecute() = {
    child.execute()

  }
}

case class ExtAggregateExec(groupingExpressions: Seq[Expression], aggregateExpressions: Seq[AggregateExpression]
                            , children: Seq[SparkPlan]) extends MultiExecNode {

  //TODO what is doProduce???
  //  override protected def doProduce(ctx: CodegenContext): String = childern.flatMap(x=>x.asInstanceOf[CodegenSupport].produce(ctx, this)).reduce(_.toString+_.toString)

  override def output: Seq[Attribute] = children.flatMap(x => x.output)

  override protected def doExecute(): RDD[InternalRow] = children(0).execute()

}
