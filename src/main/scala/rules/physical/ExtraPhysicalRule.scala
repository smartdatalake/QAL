package rules.physical

import operators.physical.{DistinctSampleExec2, UniformSampleExec2, UniformSampleExec2WithoutCI, UniversalSampleExec2}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{LogicalRDD, RDDScanExec, SparkPlan}
import scala.io.Source

class ExtraPhysicalRule {

}

case class ChangeSampleToScan(sparkSession: SparkSession,pathToSynopsesFileName:String,delimiterSynopsisFileNameAtt:String,delimiterSynopsesColumnName:String) extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    plan.transform {
      case d@DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, child) =>
        val source = Source.fromFile(pathToSynopsesFileName)
        for (line <- source.getLines()) {
          val sampleInfo = line.split(",")(1).split(delimiterSynopsisFileNameAtt)
          if (sampleInfo(0).equals("Distinct") && d.output.map(_.name).toSet.subsetOf(sampleInfo(1).split(delimiterSynopsesColumnName).toSet)
            && sampleInfo(2).toDouble >= confidence && sampleInfo(3).toDouble <= error
            && functions.map(_.toString()).toSet.subsetOf(sampleInfo(6).split(delimiterSynopsesColumnName).toSet)
            && groupingExpressions.map(_.name.split("#")(0)).toSet.subsetOf(sampleInfo(7).split(delimiterSynopsesColumnName).toSet)) {
            source.close()
            val lRRD = sparkSession.sessionState.catalog.lookupRelation(new org.apache.spark.sql.catalyst.TableIdentifier
            (line.split(",")(0), None)).children(0).asInstanceOf[LogicalRDD]
            RDDScanExec(lRRD.output, lRRD.rdd, "ExistingDistinctSampleRDD", lRRD.outputPartitioning, lRRD.outputOrdering).executeCollectPublic().toList
            return RDDScanExec(d.output, lRRD.rdd, "ExistingDistinctSampleRDD", lRRD.outputPartitioning, lRRD.outputOrdering)
          }
        }
        source.close()
        d
      case d@UniformSampleExec2WithoutCI(seed: Long, child: SparkPlan) =>
        val source = Source.fromFile(pathToSynopsesFileName)
        for (line <- source.getLines()) {
          val sampleInfo = line.split(",")(1).split(delimiterSynopsisFileNameAtt)
          if (sampleInfo(0).equals("UniformWithoutCI") && d.output.map(_.name).toSet.subsetOf(sampleInfo(1).split(delimiterSynopsesColumnName).toSet)
          ) {
            source.close()
            val lRRD = sparkSession.sessionState.catalog.lookupRelation(new org.apache.spark.sql.catalyst.TableIdentifier
            (line.split(",")(0), None)).children(0).asInstanceOf[LogicalRDD]
            return RDDScanExec(d.output, lRRD.rdd, "ExistingUniformSampleWithoutRDD", lRRD.outputPartitioning, lRRD.outputOrdering)
          }
        }
        source.close()
        d
      case d@UniformSampleExec2(functions, confidence, error, seed, child) =>
        val source = Source.fromFile(pathToSynopsesFileName)
        for (line <- source.getLines()) {
          val sampleInfo = line.split(",")(1).split(delimiterSynopsisFileNameAtt)
          if (sampleInfo(0).equals("Uniform") && d.output.map(_.name).toSet.subsetOf(sampleInfo(1).split(delimiterSynopsesColumnName).toSet)
            && sampleInfo(2).toDouble >= confidence && sampleInfo(3).toDouble <= error
            && functions.map(_.toString()).toSet.subsetOf(sampleInfo(5).split(delimiterSynopsesColumnName).toSet)) {
            source.close()
            val lRRD = sparkSession.sessionState.catalog.lookupRelation(new org.apache.spark.sql.catalyst.TableIdentifier
            (line.split(",")(0), None)).children(0).asInstanceOf[LogicalRDD]
            return RDDScanExec(d.output, lRRD.rdd, "ExistingUniformSampleRDD", lRRD.outputPartitioning, lRRD.outputOrdering)
          }
        }
        source.close()
        d
      case d@UniversalSampleExec2(functions, confidence, error, seed, joinKey, child) =>
        val source = Source.fromFile(pathToSynopsesFileName)
        for (line <- source.getLines()) {
          val sampleInfo = line.split(",")(1).split(delimiterSynopsisFileNameAtt)
          if (sampleInfo(0).equals("Universal") && d.output.map(_.name).toSet.subsetOf(sampleInfo(1).split(delimiterSynopsesColumnName).toSet)
            && sampleInfo(2).toDouble >= confidence && sampleInfo(3).toDouble <= error
            && functions.map(_.toString()).toSet.subsetOf(sampleInfo(5).split(delimiterSynopsesColumnName).toSet)
            && joinKey.map(_.name.split("#")(0)).toSet.subsetOf(sampleInfo(6).split(delimiterSynopsesColumnName).toSet)) {
            source.close()
            val lRRD = sparkSession.sessionState.catalog.lookupRelation(new org.apache.spark.sql.catalyst.TableIdentifier
            (line.split(",")(0), None)).children(0).asInstanceOf[LogicalRDD]
            return RDDScanExec(d.output, lRRD.rdd, "ExistingUniversalSampleRDD,", lRRD.outputPartitioning, lRRD.outputOrdering)
          }
        }
        source.close()
        d
      case d =>
        d
    }
  }
}