package operators.logical

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import scala.collection.Seq

case class UniformSample(function:Seq[AggregateExpression],confidence:Double, interval:Double,
                         seed: Long, child: LogicalPlan)extends UnaryNode with Serializable {
  override def output: Seq[Attribute] = child.output
  override def toString(): String = "UnifSample"+function.toString()+confidence+seed+output.toString()

}

case class UniformSampleWithoutCI(seed:Long,child:LogicalPlan)extends UnaryNode with Serializable {
  override def output: Seq[Attribute] = child.output
  override def toString(): String = "UnifSampleWithoutCI"+output.toString()

}

case class Quantile(quantileCol:AttributeReference,quantilePart:Int,confidence:Double,error:Double
                    ,seed:Long,child:LogicalPlan)
  extends UnaryNode with Serializable {
  override def output: Seq[Attribute] = Seq(AttributeReference("percent", DoubleType, false, Metadata.empty)
  (NamedExpression.newExprId, Seq.empty[String]),
    AttributeReference("index", IntegerType, false, Metadata.empty)(NamedExpression.newExprId, Seq.empty[String]))
}

case class Binning (binningCol:AttributeReference,binningPart:Int,binningStart:Double,binningEnd:Double
                    ,confidence:Double,error:Double,seed:Long,child:LogicalPlan)
  extends UnaryNode with Serializable {
  override def output: Seq[Attribute] = Seq(AttributeReference("start", DoubleType, false, Metadata.empty)
  (NamedExpression.newExprId, Seq.empty[String])
    , AttributeReference("end", DoubleType, false, Metadata.empty)(NamedExpression.newExprId, Seq.empty[String])
    , AttributeReference("count", IntegerType, false, Metadata.empty)(NamedExpression.newExprId, Seq.empty[String]))
}

case class DistinctSample(functions:Seq[AggregateExpression],confidence:Double,error:Double,seed: Long,
                          groupingExpression:Seq[NamedExpression],
                          child: LogicalPlan)extends UnaryNode with Serializable {
  override def toString(): String = "DistinctSample"+functions.toString()+confidence+error+seed+groupingExpression.toString()+output.toString()

  override def output: Seq[Attribute] = child.output
}

case class UniversalSample(functions:Seq[AggregateExpression],confidence:Double,error:Double,seed: Long,
                           joinKeys:Seq[AttributeReference],
                           child: LogicalPlan)extends UnaryNode with Serializable {
  override def output: Seq[Attribute] = child.output

  override def toString(): String = "UnivSample"+functions.toString()+confidence+error+seed+joinKeys.toString()+output.toString()
}

case class UniversalSampleWithoutKey(functions:Seq[AggregateExpression],confidence:Double,error:Double,seed: Long,
                           child: LogicalPlan)extends UnaryNode with Serializable {
  override def output: Seq[Attribute] = child.output
  override def toString(): String = "UnivSample"+functions.toString()+confidence+error+seed+"null"+output.toString()

}
