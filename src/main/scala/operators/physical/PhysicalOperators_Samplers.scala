package operators.physical

import java.io._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, _}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Count, Sum}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.sketch.CountMinSketch

import scala.collection.{Seq, mutable}
import scala.util.Random

abstract class SampleExec(child: SparkPlan) extends UnaryExecNode with CodegenSupport {

  val pathToSave="/home/hamid/TASTER/materializedSynopsis/"+this.toString()
  val path="/home/hamid/TASTER/materializedSynopsis/"
 // val path="/home/sdlhshah/spark-data/materializedSynopsis/"
  var sampleSize:Long=0
  var dataSize=0
  var fraction=.05
  val fractionStep=0.001
  val zValue=Array.fill[Double](100)(0.0)
  zValue(99)=2.58
  zValue(95)=1.96
  zValue(90)=1.64

  override def toString(): String = super.toString()
  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def usedInputs: AttributeSet = AttributeSet.empty

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  def getTargetColumnIndex(aggExp:AggregateExpression):Int={
    if(aggExp.aggregateFunction.isInstanceOf[Count]||aggExp.aggregateFunction.children(0).isInstanceOf[Count])
      return -1
    for(i <- 0 to output.size)
      if(aggExp.aggregateFunction.children(0).isInstanceOf[Cast] && output(i).name==aggExp.aggregateFunction.children(0).children(0).asInstanceOf[AttributeReference].name
      || aggExp.aggregateFunction.children(0).isInstanceOf[AttributeReference] && output(i).name==aggExp.aggregateFunction.children(0).asInstanceOf[AttributeReference].name)
        return i
    throw new Exception("The target column is not in table attributes")
  }

  def CLTCal(targetColumn:Int,data:RDD[InternalRow]):(Double,Double,Double)= {
    var n = 0.0
    var Ex:Long = 0
    var Ex2:Long = 0
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
    if(Ex2<0)
    return (Ex / n, 0.001,n)
    (Ex / n, ((Ex2 - (Ex/n)  * Ex) / (n - 1)) / n,n)
  }

  //todo set better p and m
  def hashString(string: String): Int = {
    val p = 53;
    val m = 1e9 + 9;
    var hash_value = 0;
    var p_pow = 1;
    for (c <- string) {
      hash_value = ((hash_value + (c - 'a' + 1) * p_pow) % m).toInt;
      p_pow = ((p_pow * p) % m).toInt;
    }
    hash_value
  }

  /*override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val numOutput = metricTerm(ctx, "numOutputRows")

    if (withReplacement) {
      val samplerClass = classOf[PoissonSampler[UnsafeRow]].getName
      val initSampler = ctx.freshName("initSampler")

      // Inline mutable state since not many Sample operations in a task
      val sampler = ctx.addMutableState(s"$samplerClass<UnsafeRow>", "sampleReplace",
        v => {
          val initSamplerFuncName = ctx.addNewFunction(initSampler,
            s"""
              | private void $initSampler() {
              |   $v = new $samplerClass<UnsafeRow>($upperBound - $lowerBound, false);
              |   java.util.Random random = new java.util.Random(${seed}L);
              |   long randomSeed = random.nextLong();
              |   int loopCount = 0;
              |   while (loopCount < partitionIndex) {
              |     randomSeed = random.nextLong();
              |     loopCount += 1;
              |   }
              |   $v.setSeed(randomSeed);
              | }
           """.stripMargin.trim)
          s"$initSamplerFuncName();"
        }, forceInline = true)

      val samplingCount = ctx.freshName("samplingCount")
      s"""
         | int $samplingCount = $sampler.sample();
         | while ($samplingCount-- > 0) {
         |   $numOutput.add(1);
         |   ${consume(ctx, input)}
         | }
       """.stripMargin.trim
    } else {
      val samplerClass = classOf[BernoulliCellSampler[UnsafeRow]].getName
      val sampler = ctx.addMutableState(s"$samplerClass<UnsafeRow>", "sampler",
        v => s"""
          | $v = new $samplerClass<UnsafeRow>($lowerBound, $upperBound, false);
          | $v.setSeed(${seed}L + partitionIndex);
         """.stripMargin.trim)

      s"""
         | if ($sampler.sample() != 0) {
         |   $numOutput.add(1);
         |   ${consume(ctx, input)}
         | }
       """.stripMargin.trim
    }
  }*/
}

case class UniformSampleExec2WithoutCI(seed:Long,child:SparkPlan) extends SampleExec(child) {
  override protected def doExecute(): RDD[InternalRow] = {
    val folder = (new File(path)).listFiles.filter(_.isDirectory)
    for(i <- 0 to folder.size-1){
      val sampleInfo=folder(i).getName.split(";")
      val sampleType=sampleInfo(0)
      val atts=sampleInfo(1)
      val sampleSize=sampleInfo(5).toInt
      val fraction=sampleInfo(6).toDouble
      if(sampleType=="Uniform"&&atts==child.output.map(_.name).slice(0,10).mkString("|")) {
          this.sampleSize=sampleSize
          this.fraction=fraction
          return SparkContext.getOrCreate().objectFile(path+folder(i).getName)
        }
    }
    val out= child.execute().sample(false,fraction,seed) /*.mapPartitionsWithIndexInternal { (index, iter) =>
        if(index<3)
          iter
        else
          Iterator()}*/
    this.sampleSize=out.count().toInt
    out.saveAsObjectFile(path+this.toString())
    out
  }
  override def toString(): String =
    "Uniform;" +child.output.map(_.name).slice(0,10).mkString("|") +";"+ 0 + ";" + 0 + ";" + seed + ";" + this.sampleSize+ ";"+ this.fraction + ";null"
}

case class UniformSampleExec2(functions:Seq[AggregateExpression], confidence:Double, error:Double,
                              seed: Long,
                              child: SparkPlan) extends SampleExec(child ) {
  override def toString(): String =
    "Uniform;"+child.output.map(_.name).slice(0,10).mkString("|") +";"+ confidence + ";" + error + ";" + seed + ";" + this.sampleSize+ ";"+ this.fraction + ";" + functions.mkString("_")

  var seenPartition = 0

  protected override def doExecute(): RDD[InternalRow] = {
    val folder = (new File(path)).listFiles.filter(_.isDirectory)
    for(i <- 0 to folder.size-1){
      val sampleInfo=folder(i).getName.split(";")
      val sampleType=sampleInfo(0)
      val confidence=sampleInfo(1).toDouble
      val error=sampleInfo(2).toDouble
      val sampleSize=sampleInfo(4).toInt
      val fraction=sampleInfo(5).toDouble
      if(sampleType=="Uniform")
        if(confidence>=this.confidence&&error<=this.error){
          this.sampleSize=sampleSize
          this.fraction=fraction
          return SparkContext.getOrCreate().objectFile(path+folder(i).getName)
        }
    }
    var out: RDD[InternalRow] = null
    val input = child.execute()
    while (true) {
      out = input.sample(false, fraction)
      /*.mapPartitionsWithIndexInternal { (index, iter) =>
        if(index<3)
          iter
        else
          Iterator()}*/
      //todo multiple operator on sample
      //todo without Cast
      var sampleErrorForTargetConfidence = 0.0
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
        out.saveAsObjectFile(path+this.toString())
        return out
      }

      //seenPartition += 1
      fraction += fractionStep
    }
    out
  }
}

case class DistinctSampleExec2(functions:Seq[AggregateExpression],confidence:Double,error:Double,seed: Long,
                               groupingExpression:Seq[NamedExpression],
                               child: SparkPlan) extends SampleExec(  child: SparkPlan) {

  val minNumOfOcc = 15
  private val epsOfTotalCount = 0.00001
  private val confidenceSketch = 0.99
  private val seed2 = 45423552
  val r = scala.util.Random
  r.setSeed(1234677)
  val groupValues: Seq[(Int, DataType)] = groupingExpression.map(x => {
    var index = -1
    for (i <- 0 to child.output.size - 1)
      if (child.output(i).name == x.name)
        index = i
    if (index == -1)
      throw new Exception("The grouping key is not in table columns!!!!")
    (index, x.dataType)
  })

  override def toString(): String =
    "Distinct;" + child.output.map(_.name).slice(0, 10).mkString("|") + ";" + confidence + ";" + error + ";" + seed + ";" + sampleSize + ";" + fraction + ";" + functions.mkString("_") + ";" + groupingExpression.mkString("_")

  protected override def doExecute(): RDD[InternalRow] = {
    //   output
    val folder = (new File(path)).listFiles.filter(_.isDirectory)
    /*    for (i <- 0 to folder.size - 1) {
      val sampleInfo = folder(i).getName.split(";")
      val sampleType = sampleInfo(0)
      val confidence = sampleInfo(2).toDouble
      val error = sampleInfo(3).toDouble
      val sampleSize = sampleInfo(5).toInt
      val fraction = sampleInfo(6).toDouble
      if (sampleType == "Distinct" && sampleInfo(8) == groupingExpression.mkString("_"))
        if (confidence >= this.confidence && error <= this.error) {
          println("i read from a stored sample")
          this.sampleSize = sampleSize
          this.fraction = fraction
          val t = SparkContext.getOrCreate().textFile(path + folder(i).getName)
          val p = t.collect().map(row => {
            val values = row.split("^")
            val specificInternalRow = new SpecificInternalRow(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
         //   println(row)
            for (i <- 0 to specificInternalRow.numFields - 1) {
              if (values(i) != "null" && values(i)!=""&& values(i)!=" ") {
                if (output(i).dataType == DoubleType) {
                  specificInternalRow.setDouble(i, values(i).toDouble)
                } else if (output(i).dataType == IntegerType) {
                  specificInternalRow.setInt(i, values(i).toInt)
                } else if (output(i).dataType == LongType) {
                  specificInternalRow.setLong(i, values(i).toLong)
                }  else if (output(i).dataType == FloatType) {
                  specificInternalRow.setFloat(i, values(i).toFloat)
                } else if (output(i).dataType == BooleanType) {
                  specificInternalRow.setBoolean(i, values(i).toBoolean)
                }  else if (output(i).dataType == IntegerType) {
                  specificInternalRow.setInt(i, values(i).toInt)
                } else if (output(i).dataType == TimestampType) {
                  specificInternalRow.setLong(i, 0.toLong)
                }
                else
                  specificInternalRow.update(i, UTF8String.fromString(values(i)))
              }
            }
            val x = UnsafeProjection.create(output.map(x => x.asInstanceOf[AttributeReference].dataType).toArray)
            x(specificInternalRow)
          })
          return SparkContext.getOrCreate().parallelize(p)
        }
    }*/
    var out: RDD[InternalRow] = null
    import org.apache.spark.sql.functions.rand
    val input = child.execute()
    //todo null are counted
    dataSize = input.count().toInt
    while (true) {
      out = input.mapPartitionsWithIndex { (index, iter) => {
        //      println("in sample")
        val sketch: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
        //var sketch = CountMinSketch.create(epsOfTotalCount, confidenceSketch, seed2)
        iter.flatMap { row =>
          val tempGroupKey = row.get(groupValues(0)._1, groupValues(0)._2)
          if (tempGroupKey == null)
            List()
          else {
            var thisRowKey: String = tempGroupKey.toString;
            ///  println(thisRowKey)
            //groupValues.foreach(x => thisRowKey = thisRowKey + row.get(x._1, x._2));
            val curCount = sketch.getOrElse(thisRowKey, 0)
            // val curCount=sketch.estimateCount(thisRowKey);
            if (curCount > 0) {
              if (curCount < 2 * minNumOfOcc) {
                sketch.update(thisRowKey, sketch.getOrElse(thisRowKey, 0) + 1)
                //sketch.add(thisRowKey)
                List(row)
              } else {
                val newRand = r.nextDouble
                if (newRand < fraction) {
                  List(row)
                } else {
                  List()
                }
              }
            } else {
              sketch.put(thisRowKey, 1)
              //  sketch.add(thisRowKey)
              List(row)
            }
          }
        }
      }
      }
      /*out = input.mapPartitionsWithIndex { (index, iter) =>
        var sketch = CountMinSketch.create(epsOfTotalCount, confidenceSketch, seed2)
        iter.flatMap { row =>
          val tempGroupKey = row.get(groupValues(0)._1, groupValues(0)._2)
          if (tempGroupKey == null)
            List()
          else {
            var thisRowKey: String = tempGroupKey.toString;
            ///  println(thisRowKey)
            //groupValues.foreach(x => thisRowKey = thisRowKey + row.get(x._1, x._2));
            val curCount = sketch.estimateCount(thisRowKey);
            if (curCount > 0) {
              if (curCount < 2 * minNumOfOcc) {
                sketch.add(thisRowKey)
                List(row)
              } else {
                val newRand = r.nextDouble
                if (newRand < fraction) {
                  List(row)
                } else {
                  List()
                }
              }
            } else {
              sketch.add(thisRowKey)
              List(row)
            }
          }
        }*/
      /*  out = input.flatMap {  iter =>
          var thisRowKey: String = iter.get(groupValues(0)._1, groupValues(0)._2).toString;

          //groupValues.foreach(x => thisRowKey = thisRowKey + iter.get(x._1, x._2));
          val curCount = sketch.estimateCount(thisRowKey);
          if (curCount > 0) {
            if (curCount < minNumOfOcc) {
              sketch.add(thisRowKey)
              List(iter)
            } else {
              val newRand = r.nextDouble
              if (newRand < fraction) {
                sketch.add(thisRowKey)
                List(iter)
              } else {
                List()
              }
            }
          } else {
            sketch.add(thisRowKey)
            List(iter)
          }

      }*/
      //out.cache()
      //this.sampleSize=out.count()

      println("i store a sample")
      out.map(x => {
        var stringRow = ""
        for (i <- 0 to x.numFields - 1) {
          val value = x.get(i, output(i).dataType)
          if (value == null) {
            stringRow += ","
          } else
            stringRow += x.get(i, output(i).dataType) + ","
        }
        stringRow.dropRight(1)
      }).saveAsTextFile(path + this.toString())
      return out
      val (appMean, appVariance, sampleSize) = CLTCal(getTargetColumnIndex(functions(0)), out)
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
        out.saveAsObjectFile(path + this.toString())
        return out
      }
      fraction += fractionStep
    }
    out
  }
}

case class UniversalSampleExec2(functions:Seq[AggregateExpression], confidence:Double, error:Double, seed: Long,
                                joinKey:Seq[AttributeReference],
                                child: SparkPlan) extends SampleExec(child: SparkPlan) {
  val rand = new Random(seed)
  val a = math.abs(rand.nextInt())
  val b = math.abs(rand.nextInt())
  val s = math.abs(rand.nextInt())
  val joinAttrs: Seq[(Int, DataType)] = joinKey.map(x => {
    var index = -1
    for (i <- 0 to child.output.size - 1)
      if (child.output(i).name == x.name)
        index = i
    if (index == -1)
      throw new Exception("The grouping key is not in table columns!!!!")
    (index, x.dataType)
  })

  override def toString(): String = {
    if (functions == null)
      return "Universal;" + child.output.map(_.name).slice(0, 10).mkString("|") + ";" + confidence + ";" + error + ";" + seed + ";" +sampleSize+";"+ fraction + ";" + "null" + ";" + joinKey.mkString("_")
    "Universal;" + child.output.map(_.name).slice(0, 10).mkString("|") + ";" + confidence + ";" + error + ";" + seed + ";" +sampleSize+";"+ fraction + ";" + functions.mkString("_") + ";" + joinKey.mkString("_")
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val folder = (new File(path)).listFiles.filter(_.isDirectory)
    for (i <- 0 to folder.size - 1) {
      val sampleInfo = folder(i).getName.split(";")
      val sampleType = sampleInfo(0)
      val confidence = sampleInfo(2).toDouble
      val error = sampleInfo(3).toDouble
      val seed=sampleInfo(4).toDouble
      val sampleSize = sampleInfo(5).toInt
      val fraction = sampleInfo(6).toDouble
      if (sampleType == "Universal" && sampleInfo(8) == joinKey.mkString("_"))
        if (confidence >= this.confidence && error <= this.error) {
          this.sampleSize = sampleSize
          this.fraction = fraction
          return SparkContext.getOrCreate().objectFile(path + folder(i).getName)
        }
    }
    //todo multiple join key
    var out: RDD[InternalRow] = null
    val input = child.execute()
    //todo null are counted
    val dataSize = input.count()
    while (true) {
      out = input.mapPartitionsWithIndex { (index, iter) =>
        iter.flatMap { row =>
          val join = if (joinAttrs(0)._2.isInstanceOf[StringType]) hashString(row.get(joinAttrs(0)._1, joinAttrs(0)._2).toString)
          else row.get(joinAttrs(0)._1, joinAttrs(0)._2).toString.toInt
          var t = ((join * a + b) % s) % 100
          //todo make faster
          t = if (t < 0) (t + 100) else t
          if (t < fraction * 100)
            List(row)
          else
            List()
        }
      }
      if(functions==null) {
        this.sampleSize=out.count().toInt
        out.saveAsObjectFile(path + this.toString())
        return out
      }
      val (appMean, appVariance, sampleSize) = CLTCal(getTargetColumnIndex(functions(0)), out)
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
        out.saveAsObjectFile(path + this.toString())
        //out.saveAsTextFile(path + this.toString())

        return out
      }
    //  fraction += fractionStep
    }
    out
  }
}
