package operators.physical

import java.io._

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, _}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Count, Sum}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{CodegenSupport, _}
import org.apache.spark.sql.types._
import org.apache.spark.util.sketch.CountMinSketch

import scala.collection.{Seq, mutable}
import scala.util.Random
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ListBuffer

case class PProject(projectList: Seq[NamedExpression], child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  protected override def doExecute(): RDD[InternalRow] = {
    val folder = (new File("/home/hamid/temp/")).listFiles.filter(_.isDirectory)
    for (i <- 0 to folder.size - 1) {
      if (folder(i).getName == "asd") {
        val t = SparkContext.getOrCreate().textFile("/home/hamid/temp/" + folder(i).getName)
        val p = t.collect().map(row => {
          val values = row.split(",")
          val specificInternalRow = new SpecificInternalRow(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
          for (i <- 0 to specificInternalRow.numFields - 1)
            specificInternalRow.update(i, UTF8String.fromString(values(i)))
          val x = UnsafeProjection.create(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
          x(specificInternalRow)
        })
        return SparkContext.getOrCreate().parallelize(p)
      }
    }
    val out = child.execute().mapPartitionsWithIndex { (index, iter) =>
      val project = UnsafeProjection.create(projectList, child.output,
        subexpressionEliminationEnabled)
      project.initialize(index)
      iter.map(project)
    }
    //val stringEncoder =Encoders.STRING
    //   val stringExprEncoder = stringEncoder.asInstanceOf[ExpressionEncoder[(String)]]
    //   val attrs = Seq(DslSymbol('acheneID).string, DslSymbol('lat).string,DslSymbol('lon).string,DslSymbol('province).string,DslSymbol('numberOfEmployees).string)
    //  out.map(x=>stringExprEncoder.resolveAndBind(attrs).fromRow(x)).take(10).foreach(println)
    output
    out.map(x => {
      var stringRow = ""
      for (i <- 0 to x.numFields - 1) {
        stringRow += x.get(i, output(i).dataType) + "#_"
      }
      stringRow.dropRight(1)
    }).repartition(1).saveAsTextFile("/home/hamid/temp/asd")
    out
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

}

