package operators.physical

import definition.Paths.sketchesMaterialized

import java.io.PrintWriter
import java.sql.DriverManager
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, NamedExpression, SortOrder, SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.{DoubleType, IntegerType, Metadata}
import sketch.{DyadicRanges, MultiDyadicRanges}

import scala.collection.Seq
import scala.collection.mutable.ListBuffer
case class RDDScanProteusExec(
                        output: Seq[Attribute],
                       // rdd: RDD[InternalRow],
                             dir:String,
                        name: String,
                        override val outputPartitioning: Partitioning = UnknownPartitioning(0),
                        override val outputOrdering: Seq[SortOrder] = Nil) extends LeafExecNode {


  override val nodeName: String = s"Scan Proteus $name"

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  protected override def doExecute(): RDD[InternalRow] = {
    val conf = new Configuration()
    //conf.set("fs.defaultFS", "hdfs://192.168.30.147:8020")
    val fs = FileSystem.get(conf)
    val output = fs.create(new Path(dir + name + ".csv"))
    val writer = new PrintWriter(output)
    Class.forName("org.apache.calcite.avatica.remote.Driver")
    val connection = DriverManager.getConnection("jdbc:avatica:remote:url=http://diascld32.iccluster.epfl.ch:18007;serialization=PROTOBUF", "sdlhshah", "Shah13563556")
    val t = connection.createStatement.executeQuery("select * from " + name)
    val columnCNT = t.getMetaData.getColumnCount
    while (t.next()) {
      var row = ""
      for (i <- 1 to columnCNT - 1)
        row += t.getString(i) + ";"
      row += t.getString(columnCNT)
      try
        writer.write(row + "\n")
      finally
        writer.close()
    }
    connection.close()
    val numOutputRows = longMetric("numOutputRows")
    sqlContext.read.format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .load(dir + name + ".csv").rdd.map(_.asInstanceOf[InternalRow])


    /*t.next
    System.out.println(t.getMetaData.getColumnName(1))
    System.out.println(t.getMetaData.getCatalogName(1))
    System.out.println(t.getMetaData.getColumnClassName(1))
    System.out.println(t.getMetaData.getColumnLabel(1))
    System.out.println(t.getMetaData.getColumnType(1))
    System.out.println(t.getMetaData.getColumnTypeName(1))
    System.out.println(t.getMetaData.getSchemaName(1))
    System.out.println(t.getMetaData.getColumnName(3))
    System.out.println(t.getMetaData.getCatalogName(3))
    System.out.println(t.getMetaData.getColumnClassName(3))
    System.out.println(t.getMetaData.getColumnLabel(3))
    System.out.println(t.getMetaData.getColumnType(3))
    System.out.println(t.getMetaData.getColumnTypeName(3))
    System.out.println(t.getMetaData.getSchemaName(3))*/


  }
}

case class QuantileSampleExec(quantileColAtt:AttributeReference,quantilePart:Int, child:SparkPlan) extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = {
    val result = new ListBuffer[UnsafeRow]
    val size = child.execute().count()
    val list = child.execute.map(x => x.getInt(0)).sortBy(x => x).collect().toList
    list.takeRight(1000)
    var percent = 100 / quantilePart.toDouble
    for (i <- 0 until list.size by list.size / quantilePart) {
      val row = new SpecificInternalRow(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
      row.setDouble(0, percent)
      row.setInt(1, list(i))
      val x = UnsafeProjection.create(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
      if (percent < 100)
        result += x(row)
      percent += (100 / quantilePart)
    }
    val points = child.execute.map(x => x.getInt(0)).sortBy(x => x).collect().sliding((size / (quantilePart - 1)).toInt, (size / (quantilePart - 1)).toInt).map(x => x.last).toList

    for (point <- points) {

    }
    SparkContext.getOrCreate().parallelize(result)
  }

  override def output: Seq[Attribute] = Seq(AttributeReference("percent", DoubleType, false, Metadata.empty)
  (NamedExpression.newExprId, Seq.empty[String]),
    AttributeReference("index", IntegerType, false, Metadata.empty)(NamedExpression.newExprId, Seq.empty[String]))
}

case class BinningSampleExec(binningPart:Int, binningStart:Double, binningEnd:Double, output:Seq[Attribute], child:DyadicRangeExec)
  extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = ???
}

case class QuantileSketchExec(quantilePart:Int, output:Seq[Attribute], child:DyadicRangeExec)extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = {
    val dr = sketchesMaterialized.get(child.toString).get.asInstanceOf[DyadicRanges]
    val result = new ListBuffer[UnsafeRow]

    val points = dr.getQuantiles(quantilePart)
    var percent = 100 / quantilePart.toDouble
    for (point <- points) {
      val row = new SpecificInternalRow(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
      row.setDouble(0, percent)
      row.setInt(1, point)
      val x = UnsafeProjection.create(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
      result += x(row)
      percent += (100 / quantilePart)
    }
    SparkContext.getOrCreate().parallelize(result)
  }
}
case class BinningSketchExec(binningPart:Int, binningStart:Double, binningEnd:Double, output:Seq[Attribute], child:DyadicRangeExec)
  extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = {
    val dr = sketchesMaterialized.get(child.toString).get.asInstanceOf[DyadicRanges]
    val result = new ListBuffer[UnsafeRow]
    val bucketSize = (binningEnd - binningStart) / binningPart
    for (i <- 0 to binningPart-1) {
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
  case class ExtAggregateExec(groupingExpressions: Seq[Expression]
                              , aggregateExpressions: Seq[AggregateExpression]
                              , children: Seq[SparkPlan]) extends MultiExecNode {


    override def output: Seq[Attribute] = children.flatMap(x => x.output)

    // override def inputRDDs(): Seq[RDD[InternalRow]] = childern.flatMap(x=>x.asInstanceOf[CodegenSupport].inputRDDs())

    //TODO what is doProduce???
    //  override protected def doProduce(ctx: CodegenContext): String = childern.flatMap(x=>x.asInstanceOf[CodegenSupport].produce(ctx, this)).reduce(_.toString+_.toString)

    override protected def doExecute(): RDD[InternalRow] = {
      children(0).asInstanceOf[SparkPlan].execute()
    }
  }

/*case class ExtAggregateExec(requiredChildDistributionExpressions: Option[Seq[Expression]],
groupingExpressions: Seq[NamedExpression],
aggregateExpressions: Seq[AggregateExpression],
aggregateAttributes: Seq[Attribute],
initialInputBufferOffset: Int,
resultExpressions: Seq[NamedExpression],
child: SparkPlan)
extends MultiExecNode with CodegenSupport {
  override def childern: Seq[SparkPlan] = ???

  override def inputRDDs(): Seq[RDD[InternalRow]] = ???

  override protected def doProduce(ctx: CodegenContext): String = ???

  override protected def doExecute(): RDD[InternalRow] = ???

  override def output: Seq[Attribute] =

  override def children: Seq[SparkPlan] = ???
}*/





