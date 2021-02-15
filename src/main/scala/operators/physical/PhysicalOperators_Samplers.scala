package operators.physical

import java.io._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, _}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.types._

import scala.collection.{Seq, mutable}
import scala.util.Random
import java.io.PrintWriter
import java.util

import definition.Paths
import definition.Paths._
import org.apache.spark.SparkEnv

abstract class SampleExec(confidence: Double, error: Double, func: Seq[AggregateExpression], child: SparkPlan) extends UnaryExecNode with CodegenSupport {

  def saveAsCSV(out: RDD[InternalRow], synopsis: String): Unit = {
    return Unit
    println("the next command is saving the sample")
    val time = System.nanoTime()
    Random.setSeed(System.nanoTime())
    val name = "sample" + Random.alphanumeric.filter(_.isLetter).take(20).mkString.toLowerCase
    out.map(x => {
      var stringRow = ""
      for (i <- 0 to x.numFields - 1) {
        val value = x.get(i, output(i).dataType)
        if (value == null) {
          stringRow += delimiterParquetColumn
        } else
          stringRow += x.get(i, output(i).dataType) + delimiterParquetColumn
      }
      stringRow.dropRight(1)
    }).saveAsTextFile(pathToSaveSynopses + name /*+SparkEnv.get.executorId*/)
    ParquetNameToSynopses.put(name, synopsis)
    SynopsesToParquetName.put(synopsis, name)
    lastUsedCounter += 1
    lastUsedOfParquetSample.put(name, lastUsedCounter)
    parquetNameToHeader.put(name, getHeaderOfOutput(output))
    timeForSampleConstruction += (System.nanoTime() - time) / 1000000000
    println("I have stored the sample")
  }

  val ans2 = new util.HashMap[String, Array[Double]]
  var CIStatTable = ""
  /*  val folder = (new File(pathToCIStats)).listFiles.filter(_.isFile)
  for (i <- 0 to folder.size - 1) {
    val br = new BufferedReader(new FileReader(folder(i).getAbsolutePath))
    if (output.map(_.toAttribute.name).mkString(",") == br.readLine) {
      CIStatTable=folder(i).getName.split(".").slice(0,2).mkString(".")
      while (br.ready) {
        val key = br.readLine
        val vall = br.readLine
        val v = vall.split(",")
        val vd = new Array[Double](v.length)
        for (i <- 0 until v.length) {
          vd(i) = v(i).toDouble    new PrintWriter(pathToSaveSchema + this.toString()) {
      write(output.map(_.toAttribute.name).mkString(","));
      close
    }
        }
        ans2.put(key, vd)
      }
      br.close()
    }
  }*/
  val startSamplingRate = 5
  val stopSamplingRate = 50
  val samplingStep = 5
  var sampleSize: Long = 0
  var dataSize = 0
  val aggr = 0 //if(func!=null && func.map(_.toString()).find(x=>(x.contains("count(")||x.contains("sum("))).isDefined) 1 else 0
  /* var fraction:Double = if (func!=null) {
    findMinSample(ans2,CIStatTable,func.map(_.aggregateFunction.toString()).take(1)(0),(confidence*100).toInt,error,aggr)
  } else {
    2
  }*/
  val fraction = Paths.fractionInitialize
  val fractionStep = 0.001
  val zValue = Array.fill[Double](100)(0.0)
  zValue(99) = 2.58
  zValue(95) = 1.96
  zValue(90) = 1.64

  private def getSignature(filename: String, samplingRate: Int, attrname: String, proportionWithin: Int) = CIStatTable + "," + proportionWithin + "," + samplingRate + "," + attrname

  private def findMinSample(answers: util.HashMap[String, Array[Double]], filename: String, attrname: String, desiredConfidence: Int, desiredError: Double, aggr: Int): Int = { // aggr is 0 for avg or 1 for sum
    val proportionWithin: Int = desiredConfidence
    for (samplingRate <- startSamplingRate to stopSamplingRate by samplingStep) {
      val vall: Array[Double] = answers.get(getSignature(filename, samplingRate, attrname, proportionWithin))
      if (vall != null) {
        if (desiredError > vall(aggr)) {
          if (samplingRate > 50)
            return 50
          return samplingRate
        }
      }
    }
    return 50
  }

  override def toString(): String =
    Seq("UnknownSample", getHeaderOfOutput(output), 0, 0, fraction, "null").mkString(delimiterSynopsisFileNameAtt)

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def usedInputs: AttributeSet = AttributeSet.empty

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

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

case class UniformSampleExec2WithoutCI(seed: Long, child: SparkPlan) extends SampleExec(0, 0, null, child) {
  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().sample(false, fraction, seed) /*.mapPartitionsWithIndexInternal { (index, iter) =>
        if(index<3)
          iter
        else
          Iterator()}*/
    // sampleSize = out.count()
    //saveAsCSV(out, toString())
    //  out
  }

  override def toString(): String =
    Seq("UniformWithoutCI", getHeaderOfOutput(output), 0, 0, fraction, sampleSize, "null")
      .mkString(delimiterSynopsisFileNameAtt)

}

case class UniformSampleExec2(functions: Seq[AggregateExpression], confidence: Double, error: Double,
                              seed: Long,
                              child: SparkPlan) extends SampleExec(confidence, error, functions, child) {
  override def toString(): String =
    Seq("Uniform", getHeaderOfOutput(output), confidence, error, fraction, sampleSize
      , functions.mkString(delimiterSynopsesColumnName)).mkString(delimiterSynopsisFileNameAtt)

  var seenPartition = 0

  protected override def doExecute(): RDD[InternalRow] = {
    // var out: RDD[InternalRow] = null
    // val input = child.execute()
    //  while (true) {
    child.execute().sample(false, fraction)
    //   sampleSize = out.count()
    //  saveAsCSV(out, toString())
    //  return out
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

case class DistinctSampleExec2(functions: Seq[AggregateExpression], confidence: Double, error: Double, seed: Long,
                               groupingExpression: Seq[NamedExpression],
                               child: SparkPlan) extends SampleExec(confidence, error, functions, child: SparkPlan) {
  val r = scala.util.Random
  r.setSeed(seed)
  val groupValues: Seq[(Int, DataType)] = getAttOfExpression(groupingExpression).map(x => {
    var index = -1
    for (i <- 0 to child.output.size - 1)
      if (child.output(i).name.toLowerCase == x.find(_.isInstanceOf[AttributeReference]).get.asInstanceOf[AttributeReference].name.toLowerCase)
        index = i
    if (index == -1)
      throw new Exception("The grouping key is not in table columns!!!!")
    (index, x.dataType)
  })

  override def toString(): String =
    Seq("Distinct", getHeaderOfOutput(output), confidence, error, fraction, sampleSize
      , functions.mkString(delimiterSynopsesColumnName), getAccessedColsOfExpressions(groupingExpression).mkString(delimiterSynopsesColumnName))
      .mkString(delimiterSynopsisFileNameAtt)

  protected override def doExecute(): RDD[InternalRow] = {

    // var out: RDD[InternalRow] = null
    // val input =
    //todo null are counted
    //  while (true) {
    child.execute().mapPartitionsWithIndex { (index, iter) => {
      val sketch: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
      //var sketch = CountMinSketch.create(epsOfTotalCount, confidenceSketch, seed2)
      iter.flatMap { row =>
        val tempGroupKey = row.get(groupValues(0)._1, groupValues(0)._2)
        if (tempGroupKey == null)
          List()
        else {
          val thisRowKey: String = tempGroupKey.toString
          val curCount = sketch.getOrElse(thisRowKey, 0)
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
    //  sampleSize = out.count()
    // saveAsCSV(out, toString())
    //  return out
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
}

case class UniversalSampleExec2(functions: Seq[AggregateExpression], confidence: Double, error: Double, seed: Long
                                , joinKey: Seq[AttributeReference], child: SparkPlan) extends SampleExec(confidence
  , error, functions, child: SparkPlan) {
  val rand = new Random(seed)
  val a = math.abs(rand.nextInt())
  val b = math.abs(rand.nextInt())
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

  override def toString(): String = {
    if (functions == null)
      return Seq("Universal", getHeaderOfOutput(output), confidence, error, fraction
        , sampleSize, "null", getAccessedColsOfExpressions(joinKey).mkString(delimiterSynopsesColumnName)).mkString(delimiterSynopsisFileNameAtt)
    Seq("Universal", getHeaderOfOutput(output), confidence, error, fraction
      , functions.mkString(delimiterSynopsesColumnName), getAccessedColsOfExpressions(joinKey).mkString(delimiterSynopsesColumnName)).mkString(delimiterSynopsisFileNameAtt)
  }

  override protected def doExecute(): RDD[InternalRow] = {

    /*    val folder = (new File(pathToSaveSynopses)).listFiles.filter(_.isDirectory)
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
          return SparkContext.getOrCreate().objectFile(pathToSaveSynopses + folder(i).getName)
        }
    }*/
    //todo multiple join key
    // var out: RDD[InternalRow] = null
    //  val input = child.execute()
    //todo null are counted
    //   while (true) {
    child.execute().mapPartitionsWithIndex { (index, iter) =>
      iter.flatMap { row =>
        val join = if (joinAttrs(0)._2.isInstanceOf[StringType]) hashString(row.get(joinAttrs(0)._1, joinAttrs(0)._2).toString)
        else row.get(joinAttrs(0)._1, LongType).toString.toLong
        var t = ((join * a + b) % s) % 100
        //todo make faster
        t = if (t < 0) (t + 100) else t
        if (t < fraction * 100)
          List(row)
        else
          List()
      }
    }
    //  sampleSize = out.count()
    //    saveAsCSV(out, toString())
    //    return out


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

/*
*     val folder = (new File(pathToSaveSynopses)).listFiles.filter(_.isDirectory)
    for (i <- 0 to folder.size - 1) {
      val sampleInfo = folder(i).getName.split(";")
      val sampleType = sampleInfo(0)
      val confidence = sampleInfo(1).toDouble
      val error = sampleInfo(2).toDouble
      val sampleSize = sampleInfo(4).toInt
      val fraction = sampleInfo(5).toDouble
      if (sampleType == "Uniform")
        if (confidence >= this.confidence && error <= this.error) {
          this.sampleSize = sampleSize
          this.fraction = fraction
          return SparkContext.getOrCreate().objectFile(pathToSaveSynopses + folder(i).getName)
        }
    }
    *     val folder = (new File(pathToSaveSynopses)).listFiles.filter(_.isDirectory)
    for (i <- 0 to folder.size - 1) {

      if (folder(i).getName == this.toString()) {

        return SparkContext.getOrCreate().objectFile(pathToSaveSynopses + folder(i).getName)
      }
    }
    * */
