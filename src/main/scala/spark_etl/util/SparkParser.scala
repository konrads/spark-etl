package spark_etl.util

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LogicalPlan, UnaryNode, Union}

object SparkParser {
  /**
    * Get a list of qualified dependencies.
    */
  def getDeps(sql: String): List[QfDep] = {
    def toDep(r: UnresolvedRelation) =
      r.tableIdentifier.database
        .map(db => QfDep(r.tableIdentifier.table, Some(db)))
        .getOrElse(QfDep(r.tableIdentifier.table))

    def getDepNames(plan: LogicalPlan, soFar: List[QfDep] = List.empty): List[QfDep] = {
      plan match {
        case un: UnaryNode => getDepNames(un.child, soFar)
        case bn: BinaryNode => getDepNames(bn.right, getDepNames(bn.left, soFar))
        case ur: UnresolvedRelation => toDep(ur) :: soFar
        case u: Union => u.children.foldLeft(soFar) { case (soFar2, c) => getDepNames(c, soFar2) }
        case _ => soFar
      }
    }
    val plan = CatalystSqlParser.parsePlan(sql)
    getDepNames(plan).distinct
  }

  /**
    * Strip db prefixes.
    */
  def stripDbs(sql: String): String =
    getDeps(sql).foldLeft(sql) {
      case (soFar, dep) =>
        soFar.replace(dep.qfStr, dep.dep)
    }

  case class QfDep(dep: String, prefix: Option[String] = None) {
    def qfStr: String = prefix.map(dbId => s"$dbId.$dep").getOrElse(dep)
  }
}
