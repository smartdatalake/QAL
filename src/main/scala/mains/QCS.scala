package mains

import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical._

object QCS extends QueryEngine_Abs("QueryColumnSet") {
  def main(args: Array[String]): Unit = {
    loadTables(sparkSession)
    val queries = loadWorkloadWithIP("skyServer", sparkSession)
    for (query <- queries) {
    //  println(query)
      val qe = sparkSession.sql(query._1).queryExecution
      sparkSession.sql(query._1).show()
      println(qe.logical)
      println(qe.analyzed)
      println(qe.optimizedPlan)
      println(qe.executedPlan)
      println(qe.sparkPlan)
      println("-----------------------------------------------------------------------------")
      println(qe.logical)
      val groupKey = extractGroupByKey(qe.logical)
      val joinKey = extractJoinKey(qe.logical)
      val filters = extractFilterCon(qe.logical)
      println(groupKey)
      println(joinKey)
      println(filters)
      println("-----------------------------------------------------------------------------")
      println(qe.analyzed)
      val groupKey2 = extractGroupByKey(qe.analyzed)
      val joinKey2 = extractJoinKey(qe.analyzed)
      val filters2 = extractFilterCon(qe.analyzed)
      println(groupKey2)
      println(joinKey2)
      println(filters2)
      println("************************************************************************")

    }
  }

  def extractGroupByKey(pl: LogicalPlan): Seq[Expression] = pl match {
    case a@Aggregate(groupingExpressions: Seq[Expression], aggregateExpressions: Seq[NamedExpression], child: LogicalPlan) =>
      groupingExpressions ++ extractGroupByKey(child)
    case l: LeafNode =>
      Seq()
    case _ =>
      pl.children.flatMap(extractGroupByKey)
  }

  def extractJoinKey(pl: LogicalPlan): Seq[Expression] = pl match {
    case j@Join(left: LogicalPlan, right: LogicalPlan, joinType: JoinType, condition: Option[Expression]) =>
      Seq(condition.get) ++ extractJoinKey(left) ++ extractJoinKey(right)
    case l: LeafNode =>
      Seq()
    case _ =>
      pl.children.flatMap(extractJoinKey)
  }

  def extractFilterCon(pl: LogicalPlan): Seq[Expression] = pl match {
    case j@Filter(condition: Expression, child: LogicalPlan) =>
      Seq(condition) ++ extractFilterCon(child)
    case l: LeafNode =>
      Seq()
    case _ =>
      pl.children.flatMap(extractFilterCon)
  }

  override def readConfiguration(args: Array[String]): Unit = {}
}
