package extraSQLOperators

import operators.logical.{Binning, Quantile, UniformSampleWithoutCI}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.ReturnAnswer
import org.apache.spark.sql.types._

object extraSQLOperators {
  def execQuantile(sparkSession: SparkSession, tempQuery: String,table :String, quantileCol: String, quantilePart: Int
                   , confidence: Double, error: Double, seed: Long):String = {
    if (tempQuery.size<10) {
      var out = "["
      val scan = sparkSession.sqlContext.sql("select * from " + table).queryExecution.optimizedPlan
      var quantileColAtt: AttributeReference = null
      for (p <- scan.output.toList)
        if (p.name == quantileCol)
          quantileColAtt = p.asInstanceOf[AttributeReference]
      val optimizedPhysicalPlans = sparkSession.sessionState.planner.plan(Quantile(quantileColAtt, quantilePart, confidence, error, seed, scan)).toList(0)
      optimizedPhysicalPlans.executeCollectPublic().foreach(x => out += ("{\"percent\":" + x.get(0) + ",\"value\":" + x.get(1) + "}," + "\n"))
      return out.dropRight(2) + "]"
    }
   var out = "["
    println(sparkSession.sqlContext.sql(tempQuery).queryExecution.executedPlan)
    val plan=sparkSession.sqlContext.sql(tempQuery).queryExecution.optimizedPlan
    var quantileColAtt: AttributeReference = null
    for (p <- plan.output.toList)
      if (p.name == quantileCol)
        quantileColAtt = p.asInstanceOf[AttributeReference]
    val optimizedPhysicalPlans = sparkSession.sessionState.executePlan(Quantile(quantileColAtt, quantilePart, confidence
      , error, seed, plan)).executedPlan
    //optimizedPhysicalPlans.executeCollectPublic().foreach(x => out += (x.mkString(";") + "\n"))
    println(optimizedPhysicalPlans)
    optimizedPhysicalPlans.executeCollect().foreach(x => out += ("{\"percent\":" + x.getDouble(0) + ",\"value\":" + x.getInt(1) + "},"+"\n"))
    out=out.dropRight(2) + "]"
    println(out)
    out
  }

  def execBinning(sparkSession: SparkSession, table: String, binningCol: String, binningPart: Int
                  , binningStart: Double, binningEnd: Double, confidence: Double, error: Double, seed: Long) = {
    var out = "["
    val scan = sparkSession.sqlContext.sql("select * from " + table).queryExecution.optimizedPlan
    var binningColAtt: AttributeReference = null
    for (p <- scan.output.toList)
      if (p.name == binningCol)
        binningColAtt = p.asInstanceOf[AttributeReference]
    val optimizedPhysicalPlans = sparkSession.sessionState.planner.plan(ReturnAnswer(Binning(binningColAtt, binningPart
      , binningStart, binningEnd, confidence, error, seed, scan))).toList(0)
    //optimizedPhysicalPlans.executeCollectPublic().foreach(x => out += (x.mkString(";") + "\n"))
    optimizedPhysicalPlans.executeCollectPublic().foreach(x => out += ("{\"start\":"+x.get(0)+",\"end\":"+x.get(1)+",\"count\":"+x.get(2)+"}," + "\n"))
    out.dropRight(2) + "]"
  }

  def execDataProfile(sparkSession: SparkSession, dataProfileTable: String, confidence: Double, error: Double
                      , seed: Long) = {
    var out = "["
    val scan = sparkSession.sqlContext.sql("select * from " + dataProfileTable).queryExecution.optimizedPlan
    val optimizedPhysicalPlans = sparkSession.sessionState.planner.plan(ReturnAnswer(UniformSampleWithoutCI(seed, scan))).toList(0)
    val fraction = 10
    val schema = optimizedPhysicalPlans.schema
    val columns = optimizedPhysicalPlans.schema.toList.map(x => Array[Double](x.dataType match {
      case TimestampType =>
        0
      case StringType =>
        1
      case BooleanType =>
        2
      case DateType =>
        3
      case BinaryType =>
        4
      case DoubleType =>
        5
      case FloatType =>
        6
      case IntegerType =>
        7
      case ByteType =>
        8
      case LongType =>
        9
      case ShortType =>
        10
      case _ =>
        11
    }, 0, 0, 0, 0, 0, 0, 0, 0))
    optimizedPhysicalPlans.executeCollectPublic().foreach(x => for (i <- 0 to columns.size - 1) {
      if (!x.isNullAt(i)) {
        columns(i)(1) += 1
        columns(i)(0) match {
          case 5.0 =>
            val value = x.getDouble(i)
            if (value < columns(i)(3))
              columns(i)(3) = value
            if (value > columns(i)(4))
              columns(i)(4) = value
            columns(i)(5) += value
            columns(i)(6) += value
            columns(i)(7) += value
            columns(i)(8) += value
          case 6.0 =>
            val value = x.getFloat(i)
            if (value < columns(i)(3))
              columns(i)(3) = value
            if (value > columns(i)(4))
              columns(i)(4) = value
            columns(i)(5) += value
            columns(i)(6) += value
            columns(i)(7) += value
            columns(i)(8) += value
          case 7.0 =>
            val value = x.getInt(i)
            if (value < columns(i)(3))
              columns(i)(3) = value
            if (value > columns(i)(4))
              columns(i)(4) = value
            columns(i)(5) += value
            columns(i)(6) += value
            columns(i)(7) += value
            columns(i)(8) += value
          case 9.0 =>
            val value = x.getLong(i)
            if (value < columns(i)(3))
              columns(i)(3) = value
            if (value > columns(i)(4))
              columns(i)(4) = value
            columns(i)(5) += value
            columns(i)(6) += value
            columns(i)(7) += value
            columns(i)(8) += value
          case 10.0 =>
            val value = x.getShort(i)
            if (value < columns(i)(3))
              columns(i)(3) = value
            if (value > columns(i)(4))
              columns(i)(4) = value
            columns(i)(5) += value
            columns(i)(6) += value
            columns(i)(7) += value
            columns(i)(8) += value
          case _ =>

        }
      }
    })
    val s = new Array[String](columns.size)
    for (i <- 0 to columns.size - 1) {
      val ttype = columns(i)(0).toInt match {
        case 0 =>
          "TimeStamp"
        case 1 =>
          "String"
        case 2 =>
          "Boolean"
        case 3 =>
          "Date"
        case 4 =>
          "Binary"
        case 5 =>
          "Double"
        case 6 =>
          "Float"
        case 7 =>
          "Integer"
        case 8 =>
          "Byte"
        case 9 =>
          "Long"
        case 10 =>
          "Short"
        case _ =>
          "Unknown"
      }
      //s(i) = schema(i).name + ";" + ttype + ";" + columns(i).toList(1) * fraction + ";" + columns(i).toList(2) * fraction + ";" + columns(i).toList(3) + ";" + columns(i).toList(4) + ";" + columns(i).toList(5) / columns(i).toList(1) + ";" + columns(i).toList(6) * fraction + ";" + columns(i).toList(7) + ";" + columns(i).toList(8) * fraction
      s(i) = ("{\"name\":\""+schema(i).name + "\",\"type\":\"" + ttype + "\",\"countNonNull\":"
      + columns(i).toList(1).toInt * fraction + ",\"countDistinct\":" + columns(i).toList(2).toInt * fraction + ",\"min\":"
      + columns(i).toList(3) + ",\"max\":" + columns(i).toList(4) + ",\"avg\":" + columns(i).toList(5) / columns(i).toList(1)
      + ",\"sum\":" + columns(i).toList(6) * fraction + ",\"avgDistinct\":" + columns(i).toList(7).toInt + ",\"sumDistinct\":" + columns(i).toList(8) * fraction)+"},"

    }
    s.foreach(x => out += (x + "\n"))
    out.dropRight(2) + "]"
  }
}
