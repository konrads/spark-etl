package spark_etl.parser

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LogicalPlan, UnaryNode, Union}

object Parser {
  def getDsos(sql: String): List[String] = {
    def toStr(r: UnresolvedRelation) =
      r.tableIdentifier.database
        .map(db => s"$db.${r.tableIdentifier.table}")
        .getOrElse(r.tableIdentifier.table)

    def getDsoNames(plan: LogicalPlan, soFar: List[String] = List.empty): List[String] = {
      plan match {
        case un: UnaryNode => getDsoNames(un.child, soFar)
        case bn: BinaryNode => getDsoNames(bn.right, getDsoNames(bn.left, soFar))
        case ur: UnresolvedRelation => toStr(ur) :: soFar
        case u: Union => u.children.foldLeft(soFar) { case (soFar2, c) => getDsoNames(c, soFar2)}
        case _ => soFar
      }
    }
    val plan = CatalystSqlParser.parsePlan(sql)
    getDsoNames(plan)
  }
}
