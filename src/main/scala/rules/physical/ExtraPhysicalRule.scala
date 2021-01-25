package rules.physical

import operators.physical.{DistinctSampleExec2, UniformSampleExec2, UniformSampleExec2WithoutCI, UniversalSampleExec2}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{LogicalRDD, ProjectExec, RDDScanExec, SparkPlan}
import definition.Paths._
import jdk.nashorn.internal.ir.UnaryNode

import scala.io.Source

class ExtraPhysicalRule {

}

case class ChangeSampleToScan(sparkSession: SparkSession) extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    plan.transform {
      case d@DistinctSampleExec2(functions, confidence, error, seed, groupingExpressions, child) =>
        for (parquetNameToSynopses<- ParquetNameToSynopses.toList) {
          val sampleInfo = parquetNameToSynopses._2.split(delimiterSynopsisFileNameAtt)
          if (sampleInfo(0).equals("Distinct") && getHeaderOfOutput(d.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet)
            && sampleInfo(2).toDouble >= confidence && sampleInfo(3).toDouble <= error
            //   && functions.map(_.toString()).toSet.subsetOf(sampleInfo(6).split(delimiterSynopsesColumnName).toSet)
            && getAccessedColsOfExpressions(groupingExpressions).toSet.subsetOf(sampleInfo(7).split(delimiterSynopsesColumnName).toSet)) {
            val lRRD = sparkSession.sessionState.catalog.lookupRelation(new org.apache.spark.sql.catalyst.TableIdentifier
            (parquetNameToSynopses._1, None)).children(0).asInstanceOf[LogicalRDD]
            lastUsedCounter+=1
            lastUsedOfParquetSample.put(parquetNameToSynopses._1, lastUsedCounter)
            if (d.output.size != lRRD.output.size) {
              var counter=0
              val output = lRRD.output.map(xLRDD => {
                val p = d.output.find(xSample => getAttNameOfAtt(xSample).equals(sampleInfo(1).split(delimiterParquetColumn)(counter)))
                counter+=1
                if (p.isDefined)
                  p.get
                else
                  xLRDD
              })
              return ProjectExec(d.output, RDDScanExec(output, lRRD.rdd, parquetNameToSynopses._1, lRRD.outputPartitioning, lRRD.outputOrdering))
            }
            if (!getHeaderOfOutput(d.output).equals(sampleInfo(1))) {
              return RDDScanExec(lRRD.output.map(xLRDD => d.output.find(getAttNameOfAtt(_).equals(xLRDD.name.toLowerCase)).get), lRRD.rdd, parquetNameToSynopses._1, lRRD.outputPartitioning, lRRD.outputOrdering)
            }
            return RDDScanExec(d.output, lRRD.rdd,parquetNameToSynopses._1, lRRD.outputPartitioning, lRRD.outputOrdering)
          }
        }
        d
      case d@UniformSampleExec2WithoutCI(seed: Long, child: SparkPlan) =>
        for (parquetNameToSynopses<- ParquetNameToSynopses.toList) {
          val sampleInfo = parquetNameToSynopses._2.split(delimiterSynopsisFileNameAtt)
          if (sampleInfo(0).equals("UniformWithoutCI") && getHeaderOfOutput(d.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet)) {
            val lRRD = sparkSession.sessionState.catalog.lookupRelation(new org.apache.spark.sql.catalyst.TableIdentifier
            (parquetNameToSynopses._1, None)).children(0).asInstanceOf[LogicalRDD]
            lastUsedCounter+=1
            lastUsedOfParquetSample.put(parquetNameToSynopses._1, lastUsedCounter)
            if (d.output.size != lRRD.output.size) {
              var counter=0
              val output = lRRD.output.map(xLRDD => {
                val p = d.output.find(xSample => getAttNameOfAtt(xSample).equals(sampleInfo(1).split(delimiterParquetColumn)(counter)))
                counter+=1
                if (p.isDefined)
                  p.get
                else
                  xLRDD
              })
              return ProjectExec(d.output, RDDScanExec(output, lRRD.rdd, parquetNameToSynopses._1, lRRD.outputPartitioning, lRRD.outputOrdering))
            }
            if (!getHeaderOfOutput(d.output).equals(sampleInfo(1)))
              return RDDScanExec(lRRD.output.map(xLRDD => d.output.find(getAttNameOfAtt(_).equals(xLRDD.name.toLowerCase)).get), lRRD.rdd, parquetNameToSynopses._1, lRRD.outputPartitioning, lRRD.outputOrdering)
            return RDDScanExec(d.output, lRRD.rdd,parquetNameToSynopses._1, lRRD.outputPartitioning, lRRD.outputOrdering)
          }
        }
        d
      case d@UniformSampleExec2(functions, confidence, error, seed, child) =>
        for (parquetNameToSynopses<- ParquetNameToSynopses.toList) {
          val sampleInfo = parquetNameToSynopses._2.split(delimiterSynopsisFileNameAtt)
          if (sampleInfo(0).equals("Uniform") && getHeaderOfOutput(d.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet)
            && sampleInfo(2).toDouble >= confidence && sampleInfo(3).toDouble <= error
          //     && functions.map(_.toString()).toSet.subsetOf(sampleInfo(5).split(delimiterSynopsesColumnName).toSet)
          ) {
            val lRRD = sparkSession.sessionState.catalog.lookupRelation(new org.apache.spark.sql.catalyst.TableIdentifier
            (parquetNameToSynopses._1, None)).children(0).asInstanceOf[LogicalRDD]
            lastUsedCounter+=1
            lastUsedOfParquetSample.put(parquetNameToSynopses._1, lastUsedCounter)
            if (d.output.size != lRRD.output.size) {
              var counter=0
              val output = lRRD.output.map(xLRDD => {
                val p = d.output.find(xSample => getAttNameOfAtt(xSample).equals(sampleInfo(1).split(delimiterParquetColumn)(counter)))
                counter+=1
                if (p.isDefined)
                  p.get
                else
                  xLRDD
              })
              return ProjectExec(d.output, RDDScanExec(output, lRRD.rdd, parquetNameToSynopses._1, lRRD.outputPartitioning, lRRD.outputOrdering))
            }
            if (!getHeaderOfOutput(d.output).equals(sampleInfo(1)))
              return RDDScanExec(lRRD.output.map(xLRDD => d.output.find(getAttNameOfAtt(_).equals(xLRDD.name.toLowerCase)).get), lRRD.rdd, parquetNameToSynopses._1, lRRD.outputPartitioning, lRRD.outputOrdering)
            return RDDScanExec(d.output, lRRD.rdd, parquetNameToSynopses._1, lRRD.outputPartitioning, lRRD.outputOrdering)
          }
        }
        d
      case d@UniversalSampleExec2(functions, confidence, error, seed, joinKey, child) =>
        for (parquetNameToSynopses<- ParquetNameToSynopses.toList) {
          val sampleInfo = parquetNameToSynopses._2.split(delimiterSynopsisFileNameAtt)
          if (sampleInfo(0).equals("Universal") && getHeaderOfOutput(d.output).split(delimiterParquetColumn).toSet.subsetOf(sampleInfo(1).split(delimiterParquetColumn).toSet)
            && sampleInfo(2).toDouble >= confidence && sampleInfo(3).toDouble <= error
            //   && functions.map(_.toString()).toSet.subsetOf(sampleInfo(5).split(delimiterSynopsesColumnName).toSet)
            && getAccessedColsOfExpressions(joinKey).toSet.subsetOf(sampleInfo(6).split(delimiterSynopsesColumnName).toSet)) {
            val lRRD = sparkSession.sessionState.catalog.lookupRelation(new org.apache.spark.sql.catalyst.TableIdentifier
            (parquetNameToSynopses._1, None)).children(0).asInstanceOf[LogicalRDD]
            lastUsedCounter+=1
            lastUsedOfParquetSample.put(parquetNameToSynopses._1, lastUsedCounter)
            if (d.output.size != lRRD.output.size) {
              var counter=0
              val output = lRRD.output.map(xLRDD => {
                val p = d.output.find(xSample => getAttNameOfAtt(xSample).equals(sampleInfo(1).split(delimiterParquetColumn)(counter)))
                counter+=1
                if (p.isDefined)
                  p.get
                else
                  xLRDD
              })
              return ProjectExec(d.output, RDDScanExec(output, lRRD.rdd, parquetNameToSynopses._1, lRRD.outputPartitioning, lRRD.outputOrdering))
            }
            if (!getHeaderOfOutput(d.output).equals(sampleInfo(1)))
              return RDDScanExec(lRRD.output.map(xLRDD => d.output.find(getAttNameOfAtt(_).equals(xLRDD.name.toLowerCase)).get), lRRD.rdd, parquetNameToSynopses._1, lRRD.outputPartitioning, lRRD.outputOrdering)
            return RDDScanExec(d.output, lRRD.rdd, parquetNameToSynopses._1, lRRD.outputPartitioning, lRRD.outputOrdering)
          }
        }
        d
      case d =>
        d
    }
  }
}