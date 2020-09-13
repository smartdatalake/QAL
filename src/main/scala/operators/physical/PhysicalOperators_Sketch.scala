package operators.physical

import java.io._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BinaryComparison, EqualTo, Expression, GreaterThan, IsNotNull, LessThan, Literal, NamedExpression, SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.Seq
import scala.collection.mutable.ListBuffer
import sketch._
//todo fix toString
case class CountMinSketchExec(DELTA: Double
                              , EPS: Double
                              , SEED: Long
                              , resultExpressions:Seq[NamedExpression]
                              , conditions: Seq[Expression]
                              , sketchProjectAtt:NamedExpression
                              //todo we should define scan based on other condition or resultExpression
                              , sketchLogicalRDD:LogicalRDD) extends UnaryExecNode with CodegenSupport{
  output: Seq[Attribute]
 // val path="hdfs://145.100.59.58:9000/TASTER/materializedSynopsis"
  //val path="/home/hamid/TASTER/materializedSynopsis"
  val path="/home/sdlhshah/spark-data/materializedSynopsis"
  override def inputRDDs(): Seq[RDD[InternalRow]] = child.asInstanceOf[CodegenSupport].inputRDDs()

   override def toString(): String = ("CountMin_"+DELTA + "_" + EPS + "_" + SEED + "_" + "_" + sketchProjectAtt.name.toString())

  override protected def doProduce(ctx: CodegenContext): String = child.asInstanceOf[CodegenSupport].produce(ctx, this)

  override protected def doExecute(): RDD[InternalRow] = {


    var cmsRDD: RDD[CountMinSketch] = null
    //todo fix this -1
    var frequency:Long = -1
    //TODO add materilazioatiopn

    val folder = (new File(path)).listFiles.filter(_.isDirectory).filter(x => x.getName == this.toString())
    if (folder.size == 0) {
      cmsRDD = child.execute().mapPartitionsWithIndex((index,rowIter) => {
        val countMinS = new CountMinSketch(DELTA, EPS, SEED)
        while (rowIter.hasNext)
          if (sketchProjectAtt.dataType.isInstanceOf[StringType])
            countMinS.updateString(rowIter.next().get(0, sketchProjectAtt.dataType).asInstanceOf[UTF8String].toString)
          else if (sketchProjectAtt.dataType.isInstanceOf[NumericType])
            countMinS.update(rowIter.next().get(0, sketchProjectAtt.dataType).toString.toLong)
          else
            throw new Exception(sketchProjectAtt.dataType + " is not valid data type for countMin sketch!!!")
        Iterator(countMinS)
      })
      cmsRDD.map(x => x.serialise(x)).saveAsTextFile(path + "/" + this.toString())
    }
    else {
      //todo fix serializer
      val t = new CountMinSketch(DELTA, EPS, SEED)
      cmsRDD = sparkContext.textFile(path + "/" + this.toString()).map(x => t.deserialise(x))
    }


    val cms = cmsRDD.reduce(_ + _)
    //todo define error bound
    for (point <- conditions)
      if (point.isInstanceOf[EqualTo])
        frequency += (if (point.asInstanceOf[EqualTo].left.isInstanceOf[Literal])
          cms.get(point.asInstanceOf[EqualTo].left.asInstanceOf[Literal])
        else cms.get(point.asInstanceOf[EqualTo].right.asInstanceOf[Literal]))


    SparkContext.getOrCreate().parallelize((Seq(getUnsafeRow(resultExpressions(0), frequency))))
  }

  override def output: Seq[Attribute] = Seq(resultExpressions(0).toAttribute) //.map(_.toAttribute)

  override def child: SparkPlan = {
    //todo we allow null
    val t = conditions.filter(x => x.isInstanceOf[IsNotNull])
    if (t.size != 0)
      ProjectExec(Seq(sketchProjectAtt), FilterExec(t(0), RDDScanExec(sketchLogicalRDD.output, sketchLogicalRDD.rdd
        , "ExistingRDD", sketchLogicalRDD.outputPartitioning, sketchLogicalRDD.outputOrdering)))
    else
      ProjectExec(Seq(sketchProjectAtt), RDDScanExec(sketchLogicalRDD.output, sketchLogicalRDD.rdd
        , "ExistingRDD", sketchLogicalRDD.outputPartitioning, sketchLogicalRDD.outputOrdering))
  }

  def getUnsafeRow(exp: NamedExpression, value: Long): UnsafeRow = exp.dataType match {
    case StringType => {
      ExpressionEncoder[String].toRow(value.toString) match {
        case ur: UnsafeRow => ur
      }
    }
    case LongType =>
      ExpressionEncoder[Long].toRow(value) match {
        case ur: UnsafeRow => ur
      }
    case _ =>
      null
  }

  def getCondition(in: Expression): EqualTo = {
    if (in.isInstanceOf[EqualTo])
      return in.asInstanceOf[EqualTo]
    else
      for (t <- in.children) {
        if (getCondition(t).isInstanceOf[EqualTo])
          return t.asInstanceOf[EqualTo]
      }
    null
  }
}

case class GroupCountMinSketchExec(targetColumn:AttributeReference
                              , groupingExpression:NamedExpression
                                  , condition:Seq[Seq[And]]
                              , edge:AttributeReference
                              , DELTA: Double
                              , EPS: Double
                              , SEED: Long
                              , resultExpression:AggregateExpression
                              //todo we should define scan based on other condition or resultExpression
                              , sketchLogicalRDD:LogicalRDD) extends UnaryExecNode with CodegenSupport{
  output: Seq[Attribute]
  // val path="hdfs://145.100.59.58:9000/TASTER/materializedSynopsis"
 // val path="/home/hamid/TASTER/materializedSynopsis"
  val path="/home/sdlhshah/spark-data/materializedSynopsis"
  override def inputRDDs(): Seq[RDD[InternalRow]] = child.asInstanceOf[CodegenSupport].inputRDDs()

  override def toString(): String = ("CountMin_"+DELTA + "_" + EPS + "_" + SEED + "_" + "_" + edge.name+"_" + (if(targetColumn==null) "count" else targetColumn.name))

  override protected def doProduce(ctx: CodegenContext): String = child.asInstanceOf[CodegenSupport].produce(ctx, this)

  override protected def doExecute(): RDD[InternalRow] = {
    val name=this.toString()
    val folder = (new File(path)).listFiles.filter(_.isFile).filter(x => x.getName == name)
    val CMS: CountMinSketchGroupBy = if (folder.size == 0) {
      if (targetColumn == null) {
        child.execute().mapPartitions(rowIter => {
          val cms = new CountMinSketchGroupBy(DELTA, EPS, SEED)
          while (rowIter.hasNext) {
            val row = rowIter.next()
            if(!row.anyNull) {
              if (edge.dataType.isInstanceOf[StringType])
                cms.updateString(row.getString(0))
              else
                cms.update(row.getInt(0))
            }
          }
          Iterator(cms)
        }).reduce(_ + _)
      }
      else {
        child.execute().mapPartitions(rowIter => {
          val cms = new CountMinSketchGroupBy(DELTA, EPS, SEED)
          while (rowIter.hasNext) {
            val row = rowIter.next()
            if (!row.anyNull) {
              if (edge.dataType.isInstanceOf[StringType]) {
                var ppp = row.getString(0)
                var i = ppp.size - 1
                if (i > 4) {
                  while (!ppp(i).isUpper)
                    i = i - 1
                  ppp = ppp.substring(i)
                }
                cms.updateString(ppp, row.getInt(1))
              }
              else
                cms.update(row.getInt(0), row.getInt(1))
            }
          }
          Iterator(cms)
        }).reduce(_ + _)
      }
    } else {
      val ois = new ObjectInputStream(new FileInputStream(path +"/"+ this.toString()))
      ois.readObject.asInstanceOf[CountMinSketchGroupBy]
    }
if(folder.size==0){
    val o=new ObjectOutputStream(new FileOutputStream(path+ "/" + this.toString()))
    o.writeObject(CMS)
  }
    val ppp=new ListBuffer[UnsafeRow]
    if(condition!=null){
      val p=condition(0)(0).right.children(1).asInstanceOf[Literal].value.toString
      val x=UnsafeProjection.create(Array(groupingExpression.dataType,resultExpression.dataType))
      val row = new SpecificInternalRow(Array(groupingExpression.dataType,resultExpression.dataType))
      row.update(0,UTF8String.fromString(p))
      if(resultExpression.dataType.isInstanceOf[IntegerType])
        row.setInt(1,CMS.get(p))
      else if(resultExpression.dataType.isInstanceOf[LongType])
        row.setLong(1,CMS.get(p).toLong)
      else
        row.setDouble(1,CMS.get(p))
      ppp+=x(row)
    }
    else
    for(p<- CMS.set) {
      val x=UnsafeProjection.create(Array(groupingExpression.dataType,resultExpression.dataType))
      val row = new SpecificInternalRow(Array(groupingExpression.dataType,resultExpression.dataType))
      row.update(0,UTF8String.fromString(p))
      if(resultExpression.dataType.isInstanceOf[IntegerType])
        row.setInt(1,CMS.get(p))
      else if(resultExpression.dataType.isInstanceOf[LongType])
        row.setLong(1,CMS.get(p).toLong)
      else
        row.setDouble(1,CMS.get(p))
      ppp+=x(row)
    }
    SparkContext.getOrCreate().parallelize(ppp)
    //todo define error bound
  }

  override def output: Seq[Attribute] = Seq(groupingExpression.toAttribute,resultExpression.resultAttribute)
  override def child: SparkPlan = {
    if(targetColumn==null)
      ProjectExec(Seq(edge), RDDScanExec(sketchLogicalRDD.output, sketchLogicalRDD.rdd, "ExistingRDD"
        , sketchLogicalRDD.outputPartitioning, sketchLogicalRDD.outputOrdering))
    else
      ProjectExec(Seq(edge,targetColumn), RDDScanExec(sketchLogicalRDD.output, sketchLogicalRDD.rdd, "ExistingRDD"
        , sketchLogicalRDD.outputPartitioning, sketchLogicalRDD.outputOrdering))
    //todo we allow null
  }

  def getUnsafeRow(exp: NamedExpression, value: Long): UnsafeRow = exp.dataType match {
    case StringType => {
      ExpressionEncoder[String].toRow(value.toString) match {
        case ur: UnsafeRow => ur
      }
    }
    case LongType =>
      ExpressionEncoder[Long].toRow(value) match {
        case ur: UnsafeRow => ur
      }
    case _ =>
      null
  }

  def getCondition(in: Expression): EqualTo = {
    if (in.isInstanceOf[EqualTo])
      return in.asInstanceOf[EqualTo]
    else
      for (t <- in.children) {
        if (getCondition(t).isInstanceOf[EqualTo])
          return t.asInstanceOf[EqualTo]
      }
    null
  }
}

case class GroupByMultiDyadicRangeExec(targetColumn:AttributeReference,groupingExpression:NamedExpression, confidence: Double , error: Double
                                          , SEED: Long  , resultExpression:AggregateExpression, hyperRect: Seq[Seq[And]]
                                          , edges:Seq[AttributeReference], sketchLogicalRDD:LogicalRDD)
//todo we should define scan based on other condition or resultExpression
  extends MultiDyadicRangeExec(targetColumn, confidence, error  , SEED  , resultExpression  , hyperRect  , edges , sketchLogicalRDD:LogicalRDD) {
 // val path="/home/hamid/TASTER/materializedSynopsis"
 val path="/home/sdlhshah/spark-data/materializedSynopsis"
  override protected def doExecute(): RDD[InternalRow] = {
    //TODO add materilazioatiopn
    val edgeIndex=getGroupingAttEdgeIndex(resultExpression)
    var MDR: MultiDyadicRanges=null
    val folder = (new File(path)).listFiles.filter(_.isDirectory).filter(x => x.getName == this.toString())
    if (folder.size == 0) {
      MDR = if (targetColumn == null) {
          child.execute().mapPartitions(rowIter => {
          val multiDyadicRange = new MultiDyadicRanges(0, Integer.MAX_VALUE, l, edges.map(_.name), confidence, error, SEED)
          while (rowIter.hasNext) {
            val row = rowIter.next()
            val point = new Array[Int](l)
            for (i <- 0 to l - 1)
              point(i) = row.get(i, edges(i).dataType).asInstanceOf[Int]
            multiDyadicRange.update(point)
          }
          Iterator(multiDyadicRange)
        }).reduce(_ + _)
      }
      else {
        child.execute().mapPartitions(rowIter => {
          val multiDyadicRange = new MultiDyadicRanges(0, Integer.MAX_VALUE, l, edges.map(_.name), confidence, error, SEED)
          while (rowIter.hasNext) {
            val row = rowIter.next()
            val point = new Array[Int](l)
            for (i <- 0 to l - 1)
              point(i) = row.get(i, edges(i).dataType).asInstanceOf[Int]
            multiDyadicRange.update(point, row.get(l, targetColumn.dataType).asInstanceOf[Int])
          }
          Iterator(multiDyadicRange)
        }).reduce(_ + _)
      }
      val o=new ObjectOutputStream(new FileOutputStream(path+ "/" + this.toString()))
      o.writeObject(MDR)
    }
    else{}

    val ppp=new ListBuffer[UnsafeRow]
    for(p<- MDR.keys(edgeIndex)) {
      val x=UnsafeProjection.create(Array(groupingExpression.dataType,resultExpression.dataType))
      val row = new SpecificInternalRow(Array(groupingExpression.dataType,resultExpression.dataType))
      row.setInt(0,p)
      if(resultExpression.dataType.isInstanceOf[IntegerType])
      row.setInt(1,MDR.get(p,edgeIndex))
      else if(resultExpression.dataType.isInstanceOf[LongType])
        row.setLong(1,MDR.get(p,edgeIndex).toLong)
      else
        row.setDouble(1,MDR.get(p,edgeIndex))
      ppp+=x(row)
    }
    SparkContext.getOrCreate().parallelize(ppp)
  }

  override def output: Seq[Attribute] = Seq(groupingExpression.toAttribute,resultExpression.resultAttribute)

  def getGroupingAttEdgeIndex(att:AggregateExpression):Int={
    val t=if(att.aggregateFunction.children(0).isInstanceOf[AttributeReference])
      att.aggregateFunction.children(0).asInstanceOf[AttributeReference].name
    else
      att.aggregateFunction.children(0).children(0).asInstanceOf[AttributeReference].name
    for(i <- 0 to edges.size)
      if(edges(i).name==t)
        return i
    throw new Exception("group by key is not among MDR dimensions")
  }
}

case class NonGroupByMultiDyadicRangeExec(targetColumn:AttributeReference, confidence: Double , error: Double
                                          , SEED: Long  , resultExpression:AggregateExpression, hyperRect: Seq[Seq[And]]
                                          , edges:Seq[AttributeReference], sketchLogicalRDD:LogicalRDD)
//todo we should define scan based on other condition or resultExpression
  extends MultiDyadicRangeExec(targetColumn,  confidence, error  , SEED  , resultExpression  , hyperRect  , edges , sketchLogicalRDD:LogicalRDD) {

  override protected def doExecute(): RDD[InternalRow] = {
    //TODO add materilazioatiopn
    //val path="/home/hamid/TASTER/materializedSynopsis"
    val path="/home/sdlhshah/spark-data/materializedSynopsis"
    var MDR: MultiDyadicRanges = null
    val folder = (new File(path)).listFiles.filter(_.isDirectory).filter(x => x.getName == this.toString())
    if (folder.size == 0) {
      if (targetColumn == null) {
        MDR = child.execute().mapPartitions(rowIter => {
          val multiDyadicRange = new MultiDyadicRanges(0, Integer.MAX_VALUE, l, edges.map(_.name), confidence, error, SEED)
          while (rowIter.hasNext) {
            val row = rowIter.next()
            val point = new Array[Int](l)
            for (i <- 0 to l - 1)
              point(i) = row.get(i, edges(i).dataType).asInstanceOf[Int]
            multiDyadicRange.update(point)
          }
          Iterator(multiDyadicRange)
        }).reduce(_ + _)
      }
      else {
        MDR = child.execute().mapPartitions(rowIter => {
          val multiDyadicRange = new MultiDyadicRanges(0, Integer.MAX_VALUE, l, edges.map(_.name), confidence, error, SEED)
          while (rowIter.hasNext) {
            val row = rowIter.next()
            val point = new Array[Int](l)
            for (i <- 0 to l - 1)
              if (edges(i).dataType.isInstanceOf[DoubleType])
                point(i) = math.floor(row.get(i, edges(i).dataType).asInstanceOf[Double] * 1000).toInt
              else
                point(i) = row.get(i, edges(i).dataType).asInstanceOf[Int]
            multiDyadicRange.update(point, row.get(l, targetColumn.dataType).asInstanceOf[Int])
          }
          Iterator(multiDyadicRange)
        }).reduce(_ + _)
      }
      val o=new ObjectOutputStream(new FileOutputStream(path+ "/" + this.toString()))
      o.writeObject(MDR)
    }
    else {
      val ois = new ObjectInputStream(new FileInputStream(path+"/MultiDyadicRangesemployees#390.90.15427500315423sum(cast(employees#39 as bigint))ListBuffer(lon#42)"))
      MDR= ois.readObject.asInstanceOf[MultiDyadicRanges]
      ois.close
    }

    SparkContext.getOrCreate().parallelize(Seq(getUnsafeRow(resultExpression.resultAttribute, MDR.get(hyperRect))))
  }

  override def toString(): String = "MultiDyadicRanges"+targetColumn+confidence+error+SEED+resultExpression+edges

}

abstract class MultiDyadicRangeExec(targetColumn:AttributeReference, confidence: Double
                                          , error: Double
                                          , SEED: Long
                                          , resultExpression:AggregateExpression
                                          , hyperRect: Seq[Seq[And]]
                                          , edges:Seq[AttributeReference]
                                          //todo we should define scan based on other condition or resultExpression
                                          , sketchLogicalRDD:LogicalRDD) extends UnaryExecNode with CodegenSupport {

  val l=edges.size

  override def inputRDDs(): Seq[RDD[InternalRow]] = child.asInstanceOf[CodegenSupport].inputRDDs()

  override protected def doProduce(ctx: CodegenContext): String = child.asInstanceOf[CodegenSupport].produce(ctx, this)

  //todo fix output
  override def output: Seq[Attribute] = Seq(resultExpression.resultAttribute)

  override def child: SparkPlan = {
    //todo we allow null
    //todo notNulls
    if(targetColumn==null)
      ProjectExec(edges, RDDScanExec(sketchLogicalRDD.output, sketchLogicalRDD.rdd, "ExistingRDD"
        , sketchLogicalRDD.outputPartitioning, sketchLogicalRDD.outputOrdering))
    else
      ProjectExec(edges++Seq(targetColumn), RDDScanExec(sketchLogicalRDD.output, sketchLogicalRDD.rdd, "ExistingRDD"
        , sketchLogicalRDD.outputPartitioning, sketchLogicalRDD.outputOrdering))
  }

  def getUnsafeRow(exp: NamedExpression, value: Long): UnsafeRow = exp.dataType match {
    case StringType => {
      ExpressionEncoder[String].toRow(value.toString) match {
        case ur: UnsafeRow => ur
      }
    }
    case LongType =>
      ExpressionEncoder[Long].toRow(value) match {
        case ur: UnsafeRow => ur
      }
    case DoubleType =>
      ExpressionEncoder[Long].toRow(value) match {
        case ur: UnsafeRow => ur
      }
    case _ =>
      null
  }

  def getCondition(in: Expression): EqualTo = {
    if (in.isInstanceOf[EqualTo])
      return in.asInstanceOf[EqualTo]
    else
      for (t <- in.children) {
        if (getCondition(t).isInstanceOf[EqualTo])
          return t.asInstanceOf[EqualTo]
      }
    null
  }

  /**
   * @param in Binary comparison of less greater or equalLess or equalGreater
   * @return int that obey whether includes equal or not (Dyadic Range obeys close ranges)
   */
  def getRangeIntValue(in: BinaryComparison): Int = {
    if (in.left.isInstanceOf[Literal])
      if (in.isInstanceOf[GreaterThan])
        in.left.asInstanceOf[Literal].value.toString.toDouble.toInt - 1
      else if (in.isInstanceOf[LessThan])
        in.left.asInstanceOf[Literal].value.toString.toDouble.toInt + 1
      else
        in.left.asInstanceOf[Literal].value.toString.toDouble.toInt
    else {
      if (in.isInstanceOf[GreaterThan])
        in.right.asInstanceOf[Literal].value.toString.toDouble.toInt + 1
      else if (in.isInstanceOf[LessThan])
        in.right.asInstanceOf[Literal].value.toString.toDouble.toInt - 1
      else
        in.right.asInstanceOf[Literal].value.toString.toDouble.toInt
    }
  }
}

case class DyadicRangeExec(targetColumn:AttributeReference,DELTA: Double
                           , EPS: Double
                           , SEED: Long
                           , resultExpression:AggregateExpression
                           , ranges: Seq[And]
                           , edge:AttributeReference
                           //todo we should define scan based on other condition or resultExpression
                           , sketchLogicalRDD:LogicalRDD) extends UnaryExecNode with CodegenSupport {
  output: Seq[Attribute]
  //val path = "/home/hamid/TASTER/materializedSynopsis"
  val path="/home/sdlhshah/spark-data/materializedSynopsis"
  override def inputRDDs(): Seq[RDD[InternalRow]] = child.asInstanceOf[CodegenSupport].inputRDDs()
  override protected def doProduce(ctx: CodegenContext): String = child.asInstanceOf[CodegenSupport].produce(ctx, this)
  def getOrCreateDR():DyadicRanges={
    val folder = (new File(path)).listFiles.filter(_.isFile).filter(x => x.getName == this.toString())
    val DR = if (folder.size == 0) {
      if (targetColumn == null) {
        child.execute().mapPartitions(rowIter => {
          val dyadicRange = new DyadicRanges(0, Integer.MAX_VALUE, DELTA, EPS, SEED)
          while (rowIter.hasNext) {
            val row = rowIter.next()
            if (!row.anyNull) {
              dyadicRange.update(row.getInt(0))
            }
          }
          Iterator(dyadicRange)
        }).reduce(_ + _)
      }
      else {
        child.execute().mapPartitions(rowIter => {
          val dyadicRange = new DyadicRanges(0, Integer.MAX_VALUE, DELTA, EPS, SEED)
          while (rowIter.hasNext) {
            val row = rowIter.next()
            if (!row.anyNull) {
              dyadicRange.update(row.getInt(0), row.getInt(1))
            }
          }
          Iterator(dyadicRange)
        }).reduce(_ + _)
      }
    }
    else {
      val ois = new ObjectInputStream(new FileInputStream(path + "/" + this.toString()))
      ois.readObject.asInstanceOf[DyadicRanges]
    }
    if (folder.size == 0) {
      val o = new ObjectOutputStream(new FileOutputStream(path + "/" + this.toString()))
      o.writeObject(DR)
    }
    DR
  }
  override protected def doExecute(): RDD[InternalRow] = {
    //TODO add materilazioatiopn
    var frequency: Int = 0
    // var DR: DyadicRanges = null
    val DR = getOrCreateDR()
    val output = new ListBuffer[UnsafeRow]
    val x = UnsafeProjection.create(Array(resultExpression.dataType))
    val row = new SpecificInternalRow(Array(resultExpression.dataType))
    for (range <- ranges) {
      val left = range.left.asInstanceOf[BinaryComparison].right.asInstanceOf[Literal].value.asInstanceOf[Int]
      val right = range.right.asInstanceOf[BinaryComparison].right.asInstanceOf[Literal].value.asInstanceOf[Int]
      frequency += DR.get(left, right)
    }
    if (resultExpression.dataType.isInstanceOf[IntegerType])
      row.setInt(0, frequency)
    else if (resultExpression.dataType.isInstanceOf[LongType])
      row.setLong(0, frequency.toLong)
    else
      row.setDouble(0, frequency)
    output += x(row)
    SparkContext.getOrCreate().parallelize(output)
  }

  override def output: Seq[Attribute] = if (resultExpression==null)
    Seq(AttributeReference("quantileNull", IntegerType, false, Metadata.empty)(NamedExpression.newExprId, Seq.empty[String]))
  else Seq(resultExpression.resultAttribute)

  override def child: SparkPlan = {
    //todo we allow null
    if (targetColumn == null)
      ProjectExec(Seq(edge), RDDScanExec(sketchLogicalRDD.output, sketchLogicalRDD.rdd
        , "ExistingRDD", sketchLogicalRDD.outputPartitioning, sketchLogicalRDD.outputOrdering))
    else
      ProjectExec(Seq(edge) ++ Seq(targetColumn), RDDScanExec(sketchLogicalRDD.output, sketchLogicalRDD.rdd, "ExistingRDD"
        , sketchLogicalRDD.outputPartitioning, sketchLogicalRDD.outputOrdering))
  }

  def getUnsafeRow(exp: NamedExpression, value: Long): UnsafeRow = exp.dataType match {
    case StringType => {
      ExpressionEncoder[String].toRow(value.toString) match {
        case ur: UnsafeRow => ur
      }
    }
    case LongType =>
      ExpressionEncoder[Long].toRow(value) match {
        case ur: UnsafeRow => ur
      }
    case _ =>
      null
  }

  def getCondition(in: Expression): EqualTo = {
    if (in.isInstanceOf[EqualTo])
      return in.asInstanceOf[EqualTo]
    else
      for (t <- in.children) {
        if (getCondition(t).isInstanceOf[EqualTo])
          return t.asInstanceOf[EqualTo]
      }
    null
  }

  override def toString(): String = ("DyadicRange" + DELTA + "_" + EPS + "_" + SEED + "_" + "_" + edge.name + "_" + (if (targetColumn == null) "count" else targetColumn.name))

  /**
   * @param in Binary comparison of less greater or equalLess or equalGreater
   * @return int that obey whether includes equal or not (Dyadic Range obeys close ranges)
   */
  def getRangeIntValue(in: BinaryComparison): Int = {
    if (in.left.isInstanceOf[Literal])
      if (in.isInstanceOf[GreaterThan])
        in.left.asInstanceOf[Literal].value.toString.toDouble.toInt - 1
      else if (in.isInstanceOf[LessThan])
        in.left.asInstanceOf[Literal].value.toString.toDouble.toInt + 1
      else
        in.left.asInstanceOf[Literal].value.toString.toDouble.toInt
    else {
      if (in.isInstanceOf[GreaterThan])
        in.right.asInstanceOf[Literal].value.toString.toDouble.toInt + 1
      else if (in.isInstanceOf[LessThan])
        in.right.asInstanceOf[Literal].value.toString.toDouble.toInt - 1
      else
        in.right.asInstanceOf[Literal].value.toString.toDouble.toInt
    }
  }
}

