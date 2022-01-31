package mains

import costModel.LRUCostModel
import definition.Paths._
import rules.logical.{ApproximateInjector, pushFilterUp}
import rules.physical.SampleTransformation

import scala.collection.Seq

object LowestPlanWithWarehouseLRU extends QueryEngine_Abs("LPLRU") {


  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  def main(args: Array[String]): Unit = {
    readConfiguration(args)
    val costModel = new LRUCostModel(sparkSession)
    loadTables(sparkSession)
    val queries = loadWorkloadWithIP("skyServer", sparkSession)
    mapRDDScanRowCNT = readRDDScanRowCNT(sparkSession)

/*
    sparkSession.experimental.extraOptimizations = Seq(new pushFilterUp)
    sparkSession.sql("select first.plate, other.plate, count( other.mjd) + count( first.mjd) as nightsobserved, otherplate.programname, count( other.bestobjid) as objects from specobjall first join specobjall other on first.bestobjid = other.bestobjid join platex as firstplate on firstplate.plate = first.plate join platex as otherplate on otherplate.plate = other.plate where first.scienceprimary = 1 and other.scienceprimary = 0 and other.bestobjid > 0 group by first.plate, other.plate, otherplate.programname order by nightsobserved desc, otherplate.programname, first.plate, other.plate").show()
    sparkSession.sql("Select count(*) from first").show()
    sparkSession.sql("Select count(*) from photoobj").show()
    sparkSession.sql("Select count(*) from star").show()
    sparkSession.sql("Select count(*) from galaxy").show()
    sparkSession.sql("Select count(*) from specobjall").show()
    sparkSession.sql("Select count(*) from photoprimary").show()
    sparkSession.sql("Select count(*) from specobj").show()
    sparkSession.sql("Select count(*) from specphotoall").show()
    sparkSession.sql("Select count(*) from field").show()
    sparkSession.sql("Select count(*) from specphoto").show()
    sparkSession.sql("Select count(*) from zoospec").show()
*/


    sparkSession.experimental.extraStrategies = Seq(SampleTransformation)
    sparkSession.experimental.extraOptimizations = Seq(new ApproximateInjector(confidence, error, seed), new pushFilterUp)

    execute(queries, costModel)


    printReport(results)
    flush()
  }

  override def readConfiguration(args: Array[String]): Unit = super.readConfiguration(args)
}



















