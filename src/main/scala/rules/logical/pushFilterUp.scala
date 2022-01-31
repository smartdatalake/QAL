package rules.logical

import breeze.optimize.linear.LinearProgram
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BinaryComparison, Expression, IsNotNull, NamedExpression, Or, UnaryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, _}
import org.apache.spark.sql.catalyst.rules.Rule

import scala.collection.mutable.ListBuffer

class pushFilterUp extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case p@Project(pl@projectList, f@Filter(con@condition, c@child)) =>
      /// Filter(condition, Project(pl, c))
      Filter(condition, Project(distinct(pl , extractAttributeOfFilter(con)), c))
    case j@Join(lf@Filter(lcon@conditionl, lc@childl), rf@Filter(rcon@conditionr, rc@childr), jt@joinType, con@condition) =>
      Filter(And(lcon, rcon), Join(lc, rc, jt, con))
    case j@Join(lf@Filter(lcon@conditionl, lc@childl), rc@childr, jt@joinType, con@condition) =>
      Filter(lcon, Join(lc, rc, jt, con))
    case j@Join(lc@childr, rf@Filter(rcon@conditionr, rc@child), jt@joinType, con@condition) =>
      Filter(rcon, Join(lc, rc, jt, con))
    case f@Filter(con1@condition1, c1@Filter(con2@condition2, c2@child)) =>
      Filter(And(con1, con2), c2)
  }

  def unionAttributeReferences(aa: Seq[AttributeReference], b: Seq[AttributeReference]): Seq[AttributeReference] = {
    var out = new ListBuffer[AttributeReference]()
    out.++=(aa)
    b.foreach(x => {
      if (aa.filter(xx => xx.exprId == x.exprId).size == 0) out.+=(x)
    })
    out
  }

  def extractAttributeOfFilter(exp: Expression): Seq[AttributeReference] = {
    var atts = new ListBuffer[AttributeReference]
    exp match {
      case And(left, right) =>
        return unionAttributeReferences(extractAttributeOfFilter(left), extractAttributeOfFilter(right))
      case Or(left, right) =>
        return unionAttributeReferences(extractAttributeOfFilter(left), extractAttributeOfFilter(right))
      case _ =>
    }
    if (exp.isInstanceOf[BinaryComparison]) {
      if (exp.asInstanceOf[BinaryComparison].left.isInstanceOf[AttributeReference])
        return Seq(exp.asInstanceOf[BinaryComparison].left.asInstanceOf[AttributeReference])
      return exp.asInstanceOf[BinaryComparison].right.find(_.isInstanceOf[AttributeReference]).map(_.asInstanceOf[AttributeReference]).toSeq
    }
    Seq(exp.find(_.isInstanceOf[AttributeReference]).get.asInstanceOf[AttributeReference])
  }

  def mergeFiltersCondition(filters: Seq[Filter]): Expression = {
    if (filters.size == 1)
      return filters(0).condition
    if (filters.size == 2)
      return And(filters(0).condition, filters(1).condition)
    And(filters(0).condition, mergeFiltersCondition(filters.drop(1)))
  }

  def distinct(a: Seq[NamedExpression], b: Seq[AttributeReference]): Seq[NamedExpression] = {
    val temp =  new ListBuffer[NamedExpression]
    temp.++=(a)
    for (x <- b) {
      if (a.find(s => s.name.equalsIgnoreCase(x.name)).isEmpty)
        temp.+= (x)
    }
    temp.toSeq
  }

}
