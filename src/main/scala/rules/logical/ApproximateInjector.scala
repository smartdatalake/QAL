package rules.logical

import operators.logical.ApproximateAggregate
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Cast, Divide, Expression, Multiply, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, Count, Sum}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.types.Metadata

class ApproximateInjector(conf: Double, err: Double, seed: Long) extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // case p@Project(projectList, child) if (projectList.size == 0) =>
    //    child
    case p@Project(projectList, child: Join) =>
      child
    case agg@Aggregate(groupExpr, aggrExpr, child) =>
      ApproximateAggregate(conf, err, seed, groupExpr, aggrExpr, agg.output, child)
    /* case p@Project(projectList, child: Join) if (projectList.find(_.name.equals("ww___ww")).isEmpty && child.output.last.name.equalsIgnoreCase("ww___ww")) =>
       Project(projectList ++ Seq(child.output.last), child)
     case p@Project(projectList, child: Join) if (projectList.find(_.name.equals("ww___ww")).isDefined && child.output.last.name.equalsIgnoreCase("ww___ww")) =>
       Project(projectList.filterNot(_.name.equalsIgnoreCase("ww___ww")) ++ Seq(child.output.last), child)
     case p@Project(projectList, child) if (projectList.find(_.name.equals("ww___ww")).isEmpty && child.output.last.name.equalsIgnoreCase("ww___ww")) =>
       Project(projectList ++ Seq(child.output.last), child)
     case p@Project(projectList, child) if (projectList.count(_.name.equals("ww___ww")) == 1 && !projectList.last.name.equalsIgnoreCase("ww___ww")) =>
       Project(projectList.filterNot(_.name.equalsIgnoreCase("ww___ww")) ++ child.output.filter(_.name.equalsIgnoreCase("ww___ww")), child)
     case p@Project(projectList, child) if (projectList.count(_.name.equals("ww___ww")) == 2 && !projectList.last.name.equalsIgnoreCase("ww___ww")) =>
       Project(projectList.filterNot(_.name.equalsIgnoreCase("ww___ww")) ++ child.output.filter(_.name.equalsIgnoreCase("ww___ww")), child)
     case agg@Aggregate(groupExpr, aggrExpr, child) =>
       ApproximateAggregate(conf, err, seed, groupExpr, aggrExpr, agg.output, child)
     case a@ApproximateAggregate(conf, err, seed, groupExpr, aggrExpr, output, child)
       if (child.output.find(_.name.contains("ww___ww")).isDefined) =>
       val aa = aggrExpr.map(xx => xx.transform({
       //  case c@Cast(x, a, b) =>
       //    x
         case c@Count(x) =>
           Sum(child.output.last)
         case s@Sum(x) if (!x.isInstanceOf[Multiply] && !x.toString().contains("ww___ww")) =>
           Sum(Multiply(child.output.reverse.find(_.name.contains("ww___ww")).get, Cast(x,child.output.last.dataType,null)))
         case s@Sum(z@Multiply(x: AttributeReference, y)) if (!x.name.equalsIgnoreCase("ww___ww")) =>
           Sum(Multiply(child.output.reverse.find(_.name.contains("ww___ww")).get, Cast(z,child.output.last.dataType,null)))
       }))
    ApproximateAggregate(conf, err, seed, groupExpr, aa, output, child)
  */
    /* case a@ApproximateAggregate(conf, err, seed, groupExpr, aggrExpr, output, child)
       if (aggrExpr.map(x => x.find(_.isInstanceOf[Average]).isDefined).reduce(_ || _) && child.output.last.name.contains("ww___ww")) =>
       val aa = aggrExpr.map(xx => xx.transform({
         case a@Average(x) =>
           Divide(Sum(x), Sum(child.output.last))
         case a =>
           a
       }))
       ApproximateAggregate(conf, err, seed, groupExpr, aa, output, child)*/
    case f@Filter(condition, child: Project) if (f.output.find(_.name.equals("ww___ww")).isEmpty) =>
      Filter(condition, child)
  }
}
