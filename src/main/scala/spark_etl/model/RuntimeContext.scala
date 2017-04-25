package spark_etl.model

import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.plans.logical._
import spark_etl.ConfigError

import scala.io.Source
import scala.util.{Failure, Success, Try}
import scalaz.Scalaz._
import scalaz.Validation.FlatMap._
import scalaz._

case class RuntimeResource(resource: Resource, dsos: Set[String])  // note, dso can be Extract or previously occurring Transform

case class RuntimeTransform(transform: Transform, extracts: Set[Extract], otherDsos: Set[String])

case class RuntimeContext(runtimeTransforms: List[RuntimeTransform]) {
  def allExtracts = runtimeTransforms.flatMap(_.extracts).distinct
  def allOtherDsos = runtimeTransforms.flatMap(_.otherDsos).distinct
}

object RuntimeContext extends DefaultYamlProtocol {

  /**
    * Emphasis on *maximum* validation.
    */
  def load(conf: Config): ValidationNel[ConfigError, RuntimeContext] = {
    // get map of all extract names
    val allExtracts = conf.extracts.map(_.name.toLowerCase)

    // map available extracts for every transform (including extracts & transform predecessors)
    val (_, byTransformDsos) = conf.transforms.foldLeft((List.empty[String], Map.empty[String, List[String]])) {
      case ((predecessors, map), transform) =>
        val availableDsos = allExtracts ::: predecessors
        val predecessors2 = transform.name.toLowerCase :: predecessors
        (predecessors2, map + (transform.name.toLowerCase -> availableDsos))
    }

    val validatedTransforms = conf.transforms.map(t => validateTransform(t, byTransformDsos(t.name), conf.extracts))
    val res = validatedTransforms.map(_.map(List(_))).reduce(_ +++ _).map(RuntimeContext(_))
    res
  }

  private def loadResource(resourceUri: String): ValidationNel[ConfigError, String] = {
    val res = getClass.getResource(resourceUri)
    if (res == null)
      ConfigError(s"Failed to read resource $resourceUri").failureNel[String]
    else
      Source.fromURL(res).mkString.successNel[ConfigError]
  }

  /**
    * Load & parse sql
    * Load & parse pre_check, if specified
    * Load & parse post_check, if specified
    * Check dso dependencies
    */
  private def validateTransform(transform: Transform, availableDsos: List[String], allExtracts: List[Extract]): ValidationNel[ConfigError, RuntimeTransform] = {
    // load resources
    val validatedSql = loadValidateResource(transform.sql)
      .flatMap(validateResolvedDsos(availableDsos, s"Unresolved dsos for sql of transform ${transform.name}"))
    val validatedPreCheck = liftOpt(transform.pre_check)(loadValidateResource(_)
      .flatMap(validateResolvedDsos(availableDsos, s"Unresolved dsos for pre_check transform ${transform.name}")))
    val validatedPostCheck = liftOpt(transform.post_check)(loadValidateResource(_)
      .flatMap(validateResolvedDsos(availableDsos, s"Unresolved dsos for post_check transform ${transform.name}")))

    val runtimeTransform = (validatedSql |@| validatedPreCheck |@| validatedPostCheck) {
      (sql, preCheck, postCheck) =>
        val transform2 = transform.copy(sql = sql.resource, pre_check = preCheck.map(_.resource), post_check = postCheck.map(_.resource))
        val allUsedDsos = sql.dsos ++ preCheck.map(_.dsos).getOrElse(List.empty[String]) ++ postCheck.map(_.dsos).getOrElse(List.empty[String])
        val extracts = allExtracts.toSet.filter(e => allUsedDsos.contains(e.name.toLowerCase))
        val otherDsos = allUsedDsos -- extracts.map(_.name)
        RuntimeTransform(transform2, extracts, otherDsos)
    }
    runtimeTransform
  }

  private def liftOpt[T1, T2](opt: Option[T1])(toVal: T1 => ValidationNel[ConfigError, T2]): ValidationNel[ConfigError, Option[T2]] =
    opt match {
      case Some(r) => toVal(r).map(Some(_))
      case None => None.successNel[ConfigError]
    }

  private def loadValidateResource(resource: Resource): ValidationNel[ConfigError, Resource] =
    if (resource.uri.isEmpty && resource.contents.isEmpty)
      ConfigError("no uri/contents specified for resource").failureNel[Resource]
    else if (resource.contents.nonEmpty)
      resource.successNel[ConfigError]
    else
      loadResource(resource.uri.get).map(contents => resource.copy(contents = Some(contents)))

  private def validateResolvedDsos(availableDsos: Seq[String], errMsgPrefix: String)(res: Resource): ValidationNel[ConfigError, RuntimeResource] =
    Try(getDsos(res.contents.get)) match {
      case Success(usedDsos) =>
        val unavailables = usedDsos.toSet -- availableDsos.toSet
        if (unavailables.isEmpty)
          RuntimeResource(res, usedDsos.toSet).successNel[ConfigError]
        else
          ConfigError(s"$errMsgPrefix: ${unavailables.mkString(", ")} ").failureNel[RuntimeResource]
      case Failure(e:ParseException) =>
        ConfigError(s"Failed to parse config body of ${res.uri.get}:\n${e.getMessage}").failureNel[RuntimeResource]
      case Failure(e) =>
        ConfigError(s"Failed to parse config body of ${res.uri.get}", Some(e)).failureNel[RuntimeResource]
    }

  private def getDsos(sql: String): List[String] = {
    def getDsoNames(plan: LogicalPlan, soFar: List[String] = List.empty): List[String] = {
      plan match {
        case un: UnaryNode => getDsoNames(un.child, soFar)
        case bn: BinaryNode => getDsoNames(bn.right, getDsoNames(bn.left, soFar))
        case ur: UnresolvedRelation => ur.tableIdentifier.table :: soFar
        case u: Union => u.children.foldLeft(soFar) { case (soFar2, c) => getDsoNames(c, soFar2)}
        case _ => soFar
      }
    }
    getDsoNames(CatalystSqlParser.parsePlan(sql))
  }
}
