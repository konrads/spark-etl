package spark_etl.model

import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.plans.logical._
import spark_etl.{ConfigError, ExtractReader, LoadWriter}

import scala.io.Source
import scala.util.{Failure, Success, Try}
import scalaz.Scalaz._
import scalaz.Validation.FlatMap._
import scalaz._

case class RuntimeResource(contents: String, dsos: Set[String])  // note, dso can be Extract or previously occurring Transform

case class RuntimeExtract(org: Extract, checkRes: Option[RuntimeResource])

case class RuntimeTransform(org: Transform, sqlRes: RuntimeResource, checkRes: Option[RuntimeResource], extracts: Set[RuntimeExtract], otherDsos: Set[String])

case class RuntimeContext(runtimeExtracts: List[RuntimeExtract], runtimeTransforms: List[RuntimeTransform], extractReader: ExtractReader, loadWriter: LoadWriter) {
  def allExtracts: List[RuntimeExtract] = runtimeTransforms.flatMap(_.extracts).distinct
  def allOtherDsos: List[String] = runtimeTransforms.flatMap(_.otherDsos).distinct
}

object RuntimeContext extends DefaultYamlProtocol {

  /**
    * Emphasis on *maximum* validation.
    */
  def load(conf: Config): ValidationNel[ConfigError, RuntimeContext] = {
    // get map of all extract names
    val allExtractNames = conf.extracts.map(_.name.toLowerCase)

    // map available extracts for every transform (including extracts & transform predecessors)
    val (_, byTransformDsos) = conf.transforms.foldLeft((List.empty[String], Map.empty[String, List[String]])) {
      case ((predecessors, map), transform) =>
        val predecessors2 = transform.name.toLowerCase :: predecessors
        val availableDsos = allExtractNames ::: predecessors2
        (predecessors2, map + (transform.name.toLowerCase -> availableDsos))
    }

    val validatedExtracts = conf.extracts
      .map(e => validateExtract(e))
      .map(_.map(List(_))).reduce(_ +++ _)

    val validatedTransforms = validatedExtracts.flatMap {
      extracts =>
        conf.transforms
          .map(t => validateTransform(t, byTransformDsos(t.name), extracts))
          .map(_.map(List(_))).reduce(_ +++ _)
    }

    val validatedExtractor = instantiate[ExtractReader](conf.extract_reader.get, classOf[ExtractReader])

    val validatedTransformer = instantiate[LoadWriter](conf.load_writer.get, classOf[LoadWriter])

    (validatedExtracts |@| validatedTransforms |@| validatedExtractor |@| validatedTransformer) { (es, ts, e, t) => RuntimeContext(es, ts, e, t) }
  }

  private def loadResource(uri: String): ValidationNel[ConfigError, String] = {
    val fqUri = getClass.getResource(uri)
    if (fqUri == null)
      ConfigError(s"Failed to read resource $uri").failureNel[String]
    else
      Source.fromURL(fqUri).mkString.successNel[ConfigError]
  }

  /**
    * Load & parse check, if specified
    * Note, extract check is only dependant on the extract
    */
  private def validateExtract(extract: Extract): ValidationNel[ConfigError, RuntimeExtract] =
    extract.check match {
      case Some(checkUri) =>
        val deps = List(extract.name.toLowerCase)
        loadResource(checkUri)
          .flatMap(validateResolvedDsos(deps, s"extract check ${extract.name} (uri $checkUri)"))
          .map(rr => RuntimeExtract(extract, Some(rr)))
      case None =>
        RuntimeExtract(extract, None).successNel[ConfigError]
    }

  /**
    * Load & parse sql
    * Load & parse pre_check, if specified
    * Load & parse post_check, if specified
    * Check dso dependencies
    */
  private def validateTransform(transform: Transform, availableDsos: List[String], allExtracts: List[RuntimeExtract]): ValidationNel[ConfigError, RuntimeTransform] = {
    // load resources
    val validatedSql = loadResource(transform.sql)
      .flatMap(validateResolvedDsos(availableDsos, s"Unresolved ds'es for sql of transform ${transform.name}"))
    val validatedCheck = liftOpt(transform.check)(loadResource(_)
      .flatMap(validateResolvedDsos(availableDsos, s"Unresolved ds'es for post_check transform ${transform.name}")))

    val runtimeTransform = (validatedSql |@| validatedCheck) {
      (sql, check) =>
        val allUsedDsos = sql.dsos ++ check.map(_.dsos).getOrElse(List.empty[String])
        val extracts = allExtracts.toSet.filter(e => allUsedDsos.contains(e.org.name.toLowerCase))
        val otherDsos = allUsedDsos -- extracts.map(_.org.name)
        RuntimeTransform(transform, sql, check, extracts, otherDsos)
    }
    runtimeTransform
  }

  private def liftOpt[T1, T2](opt: Option[T1])(toVal: T1 => ValidationNel[ConfigError, T2]): ValidationNel[ConfigError, Option[T2]] =
    opt match {
      case Some(r) => toVal(r).map(Some(_))
      case None => None.successNel[ConfigError]
    }

  private def validateResolvedDsos(availableDsos: Seq[String], errMsgPrefix: String)(contents: String): ValidationNel[ConfigError, RuntimeResource] =
    Try(getDsos(contents)) match {
      case Success(usedDsos) =>
        val unavailables = usedDsos.toSet -- availableDsos.toSet
        if (unavailables.isEmpty)
          RuntimeResource(contents, usedDsos.toSet).successNel[ConfigError]
        else
          ConfigError(s"$errMsgPrefix: unexpectedly unavailable DS'es: ${unavailables.mkString(", ")} ").failureNel[RuntimeResource]
      case Failure(e: ParseException) =>
        ConfigError(s"$errMsgPrefix: failed to parse, error: ${e.getMessage}").failureNel[RuntimeResource]
      case Failure(e) =>
        ConfigError(s"$errMsgPrefix: failed to parse", Some(e)).failureNel[RuntimeResource]
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

  private def instantiate[T](paramConstr: ParametrizedConstructor, parentClass: Class[_]): ValidationNel[ConfigError, T] = {
    Try {
      val clazz = Class.forName(paramConstr.`class`)
      if (parentClass.isAssignableFrom(clazz)) {
        val constructor = clazz.getConstructors()(0)
        constructor.newInstance(paramConstr.params.get).asInstanceOf[T].successNel[ConfigError]
      } else {
        ConfigError(s"Failed to cast class ${paramConstr.`class`} to ${parentClass.getName}").failureNel[T]
      }
    } match {
      case scala.util.Success(validated) => validated
      case scala.util.Failure(e) => ConfigError(s"Failed to instantiate class ${paramConstr.`class`} with params: ${paramConstr.params}", Some(e)).failureNel[T]
    }
  }
}
