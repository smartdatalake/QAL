package rules.physical

import operators.physical._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, RDDScanExec, SparkPlan}
import definition.Paths._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.apache.spark.sql.catalyst.InternalRow

class ExtraPhysicalRule {

}

case class ExtendProjectList(extraAtt: Seq[String]) extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    plan.transform {
      case p@ProjectExec(projectList, child) if (child.output.filter(x => (projectList.find(_.equals(x)).isEmpty
        && extraAtt.contains(tableName.get(x.toString().toLowerCase).get + "." + x.name.split("#")(0).toLowerCase))).size > 0) =>
        ProjectExec((projectList ++ child.output.filter(x => (projectList.find(_.toString().equalsIgnoreCase(x.toString())).isEmpty && extraAtt.contains(tableName.get(x.toString().toLowerCase).get + "." + x.name.split("#")(0).toLowerCase)))), child)
      // p
      // case s@UniformSampleExec2(a,b,c,d)=>

    }
  }
}


case class ChangeSampleToScan(sparkSession: SparkSession) extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    plan.transform {
      case s: SampleExec =>
        for (parquetNameToSynopses <- ParquetNameToSynopses.toList)
          if (isMoreAccurate(s, parquetNameToSynopses._2))
            return makeExecutionNode(s, parquetNameToSynopses)
        s
      /*    case d@DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, child) =>
            for (parquetNameToSynopses <- ParquetNameToSynopses.toList) {
              val sampleInfo = parquetNameToSynopses._2.split(delimiterSynopsisFileNameAtt)
              if (sampleInfo(0).equals("Distinct") && getHeaderOfOutput(d.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet)
                && sampleInfo(2).toDouble >= confidence && sampleInfo(3).toDouble <= error
                //   && functions.map(_.toString()).toSet.subsetOf(sampleInfo(6).split(delimiterSynopsesColumnName).toSet)
                && getAccessedColsOfExpressions(groupingExpressions).toSet.subsetOf(sampleInfo(7).split(delimiterSynopsesColumnName).toSet)
                && sampleInfo(8).equals(d.joins))
                return makeExecutionNode(d, parquetNameToSynopses)
            }
            d
          case d@UniformSampleExec2WithoutCI(seed: Long, child: SparkPlan) =>
            for (parquetNameToSynopses <- ParquetNameToSynopses.toList) {
              val sampleInfo = parquetNameToSynopses._2.split(delimiterSynopsisFileNameAtt)
              if (sampleInfo(0).equals("UniformWithoutCI") && getHeaderOfOutput(d.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet)
                && sampleInfo(7).equals(d.joins))
                return makeExecutionNode(d, parquetNameToSynopses)
            }
            d
          case d@UniformSampleExec2(functions, confidence, error, seed, child) =>
            for (parquetNameToSynopses <- ParquetNameToSynopses.toList) {
              val sampleInfo = parquetNameToSynopses._2.split(delimiterSynopsisFileNameAtt)
              if (sampleInfo(0).equals("Uniform") && (if (d.output.size == 0) true else getHeaderOfOutput(d.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet))
                && sampleInfo(2).toDouble >= confidence && sampleInfo(3).toDouble <= error
                //     && functions.map(_.toString()).toSet.subsetOf(sampleInfo(5).split(delimiterSynopsesColumnName).toSet)
                && sampleInfo(7).equals(d.joins))
                return makeExecutionNode(d, parquetNameToSynopses)
              else if (sampleInfo(0).equals("Universal") && getHeaderOfOutput(d.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet)
                && sampleInfo(2).toDouble >= confidence && sampleInfo(3).toDouble <= error
                //   && functions.map(_.toString()).toSet.subsetOf(sampleInfo(5).split(delimiterSynopsesColumnName).toSet)
                && sampleInfo(7).equals(d.joins))
                return makeExecutionNode(d, parquetNameToSynopses)
              else if (sampleInfo(0).equals("Distinct") && getHeaderOfOutput(d.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet)
                && sampleInfo(2).toDouble >= confidence && sampleInfo(3).toDouble <= error
                //   && functions.map(_.toString()).toSet.subsetOf(sampleInfo(6).split(delimiterSynopsesColumnName).toSet)
                && sampleInfo(8).equals(d.joins))
                return makeExecutionNode(d, parquetNameToSynopses)
            }
            d
          case d@UniversalSampleExec2(functions, confidence, error, seed, joinKey, child) =>
            for (parquetNameToSynopses <- ParquetNameToSynopses.toList) {
              val sampleInfo = parquetNameToSynopses._2.split(delimiterSynopsisFileNameAtt)
              if (sampleInfo(0).equals("Universal") && getHeaderOfOutput(d.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet)
                && sampleInfo(2).toDouble >= confidence && sampleInfo(3).toDouble <= error
                //   && functions.map(_.toString()).toSet.subsetOf(sampleInfo(5).split(delimiterSynopsesColumnName).toSet)
                && sampleInfo(7).equals(d.joins)
                && getAccessedColsOfExpressions(joinKey).toSet.subsetOf(sampleInfo(6).split(delimiterSynopsesColumnName).toSet))
                return makeExecutionNode(d, parquetNameToSynopses)
            }
            d*/
      case d =>
        d
    }
  }


  def isMoreAccurate(sampleExec: SampleExec, sampleInf: String): Boolean = sampleExec match {
    case u@UniformSampleExec2(functions, confidence, error, seed, child) =>
      val sampleInfo = sampleInf.split(delimiterSynopsisFileNameAtt)
      if (sampleInfo(0).equals("Uniform") && (if (u.output.size == 0) {
        val tttt = tableName.get(u.child.asInstanceOf[ProjectExec].child.output(0).toString().toLowerCase).get.split("_")(0)
        if (sampleInfo(1).split(delimiterParquetColumn).map(_.split("\\.")(0).split("_")(0)).find(_.equalsIgnoreCase(tttt)).isDefined)
          true
        else false
      } else getHeaderOfOutput(u.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet))
        && sampleInfo(2).toDouble >= confidence && sampleInfo(3).toDouble <= error
        //     && functions.map(_.toString()).toSet.subsetOf(sampleInfo(5).split(delimiterSynopsesColumnName).toSet)
        && sampleInfo(7).equals(u.joins))
        return true
      else if (sampleInfo(0).equals("Universal") && getHeaderOfOutput(u.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet)
        && sampleInfo(2).toDouble >= confidence && sampleInfo(3).toDouble <= error
        //   && functions.map(_.toString()).toSet.subsetOf(sampleInfo(5).split(delimiterSynopsesColumnName).toSet)
        && sampleInfo(7).equals(u.joins))
        return true
      else if (sampleInfo(0).equals("Distinct") && getHeaderOfOutput(u.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet)
        && sampleInfo(2).toDouble >= confidence && sampleInfo(3).toDouble <= error
        //   && functions.map(_.toString()).toSet.subsetOf(sampleInfo(6).split(delimiterSynopsesColumnName).toSet)
        && sampleInfo(8).equals(u.joins))
        return true
      else false
    case d@DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, child) =>
      val sampleInfo = sampleInf.split(delimiterSynopsisFileNameAtt)
      if (sampleInfo(0).equals("Distinct")
        && getHeaderOfOutput(d.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet)
        && sampleInfo(2).toDouble >= confidence && sampleInfo(3).toDouble <= error
        //   && functions.map(_.toString()).toSet.subsetOf(sampleInfo(6).split(delimiterSynopsesColumnName).toSet)
        && getAccessedColsOfExpressions(groupingExpressions).toSet.subsetOf(sampleInfo(7).split(delimiterSynopsesColumnName).toSet)
        && sampleInfo(8).equals(d.joins))
        true else
        false
    case u@UniversalSampleExec2(functions, confidence, error, seed, joinKey, child) =>
      val sampleInfo = sampleInf.split(delimiterSynopsisFileNameAtt)
      if (sampleInfo(0).equals("Universal") && getHeaderOfOutput(u.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet)
        && sampleInfo(2).toDouble >= confidence && sampleInfo(3).toDouble <= error
        //   && functions.map(_.toString()).toSet.subsetOf(sampleInfo(5).split(delimiterSynopsesColumnName).toSet)
        && sampleInfo(7).equals(u.joins)
        && getAccessedColsOfExpressions(joinKey).toSet.subsetOf(sampleInfo(6).split(delimiterSynopsesColumnName).toSet))
        true
      else
        false
    case u@UniformSampleExec2WithoutCI(seed, child) =>
      val sampleInfo = sampleInf.split(delimiterSynopsisFileNameAtt)
      if (sampleInfo(0).equals("UniformWithoutCI") && getHeaderOfOutput(u.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet)
        && sampleInfo(7).equals(u.joins))
        true else
        false
    case _ => false
  }

  def makeExecutionNode(d: operators.physical.SampleExec, parquetNameToSynopses: (String, String)): SparkPlan = {
    val sampleInfo = parquetNameToSynopses._2.split(delimiterSynopsisFileNameAtt)
    val LRDD_Output = sampleToOutput.get(parquetNameToSynopses._1).get
    lastUsedCounter += 1
    lastUsedOfParquetSample.put(parquetNameToSynopses._1, lastUsedCounter)
    val obj: RDD[InternalRow] = SparkContext.getOrCreate().objectFile(pathToSaveSynopses + parquetNameToSynopses._1 + ".obj") //.repartition(1)
    //System.out.println(parquetNameToSynopses._2 + " has been read from the file")
    if (d.output.size != LRDD_Output.size) {
      var counter = 0
      val output = LRDD_Output.map(xLRDD => {
        val p = d.output.find(xSample => getAttNameOfAtt(xSample).equals(sampleInfo(1).split(delimiterParquetColumn)(counter)))
        counter += 1
        if (p.isDefined)
          p.get
        else
          xLRDD
      })
      ProjectExec(d.output, RDDScanExec(output, obj, parquetNameToSynopses._1, UnknownPartitioning(1), Nil))
    }
    else if (!getHeaderOfOutput(d.output).equals(sampleInfo(1))) {
      tableName
      RDDScanExec(LRDD_Output.map(xLRDD => d.output.find(x=>getAttNameOfAtt(x).toString().toLowerCase.equals(getAttNameOfAtt(xLRDD).toString().toLowerCase)).get), obj, parquetNameToSynopses._1, UnknownPartitioning(1), Nil)
    }
    else RDDScanExec(d.output, obj, parquetNameToSynopses._1, UnknownPartitioning(1), Nil)
  }

}