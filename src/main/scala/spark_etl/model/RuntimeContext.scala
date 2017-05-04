package spark_etl.model

import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.plans.logical._
import spark_etl.util._
import spark_etl.{ConfigError, ExtractReader, LoadWriter}

import scala.io.Source
import scala.util.{Failure, Success, Try}
import scalaz.Scalaz._
import scalaz.Validation.FlatMap._
import scalaz._

case class RuntimeExtract(org: Extract, checkRes: Option[String])

case class RuntimeTransform(org: Transform, sqlContents: String, checkContents: Option[String])

class RuntimeContext(extracts: List[RuntimeExtract], transforms: List[RuntimeTransform], val loads: List[Load], val extractReader: ExtractReader, val loadWriter: LoadWriter, depTree: DepTree, conf: Config) {
  def allExtracts: List[RuntimeExtract] =
    (for {
      r <- depTree.forChildType(E)
      e <- extracts
      if r.child.id == e.org.name
    } yield e).toList

  def allTransforms: List[RuntimeTransform] =
    (for {
      r <- depTree.forChildType(T)
      t <- transforms
      if r.child.id == t.org.name
    } yield t).toList

  def allLoads: List[Load] =
    (for {
      r <- depTree.forParentType(L)
      l <- loads
      if r.parent.id == l.name
    } yield l).toList

}

object RuntimeContext extends DefaultYamlProtocol {

  /**
    * Emphasis on *maximum* validation.
    */
  def load(_conf: Config, env: Map[String, String]): ValidationNel[ConfigError, RuntimeContext] = {
    // depTree, with the known universe
    val conf = toLowerCase(_conf)
    val allExtractNames = conf.extracts.map(_.name)
    val allTransformNames = conf.transforms.map(_.name)
    val allLoadNames = conf.loads.map(_.name)
    val depTree = new DepTree()

    // setup all candidate deps, note: checks relies on predecessors and themselves
    conf.extracts.foreach(e => depTree.addCandidate(Rel(Node(e.name, E), Node(e.name, Echeck))))

    allTransformNames.foldLeft(List.empty[String]) {
      case (predTs, t) =>
        // E -> T
        allExtractNames.foreach(eName => depTree.addCandidate(Rel(Node(eName, E), Node(t, T))))
        predTs.foreach(predT => depTree.addCandidate(Rel(Node(predT, T), Node(t, T))))
        val predTs2 = predTs :+ t
        // E -> Tcheck
        allExtractNames.foreach(eName => depTree.addCandidate(Rel(Node(eName, E), Node(t, Tcheck))))
        predTs2.foreach(predT => depTree.addCandidate(Rel(Node(predT, T), Node(t, Tcheck))))
        // T -> L
        conf.loads.foreach(l => depTree.addCandidate(Rel(Node(t, T), Node(l.name, L))))
        predTs2
    }

    // read in entities and add their deps
    val regExtracts = conf.extracts
      .map(e => registerExtractDeps(e, depTree, env))
      .map(_.map(List(_))).reduce(_ +++ _)

    val regTransforms = conf.transforms
      .map(t => registerTransformDeps(t, depTree, env))
      .map(_.map(List(_))).reduce(_ +++ _)

    conf.loads.foreach(l => depTree.addActual(l.source, Node(l.name, L)))

    val validatedDepTree = validateDepTree(depTree)

    val validatedExtractor = instantiate[ExtractReader](conf.extract_reader.get, classOf[ExtractReader])

    val validatedTransformer = instantiate[LoadWriter](conf.load_writer.get, classOf[LoadWriter])

    (regExtracts |@| regTransforms |@| validatedExtractor |@| validatedTransformer |@| validatedDepTree) { (es, ts, e, t, dt) => new RuntimeContext(es, ts, conf.loads, e, t, depTree, conf)  }
  }

  private def toLowerCase(conf: Config): Config =
    conf.copy(
      extracts = conf.extracts.map(e => e.copy(name = e.name.toLowerCase)),
      transforms = conf.transforms.map(t => t.copy(name = t.name.toLowerCase)),
      loads = conf.loads.map(l => l.copy(source = l.source.toLowerCase))
    )

  private def loadResource(uri: String, env: Map[String, String]): ValidationNel[ConfigError, String] = {
    val fqUri = getClass.getResource(uri)
    if (fqUri == null)
      ConfigError(s"Failed to read resource $uri").failureNel[String]
    else {
      val contents = Source.fromURL(fqUri).mkString
      val contents2 = env.foldLeft(contents) { case (soFar, (k, v)) => soFar.replaceAll("\\$\\{" + k + "\\}", v) }
      contents2.successNel[ConfigError]
    }
  }

  /**
    * Load & parse check, if specified
    * Note, extract check is only dependant on the extract
    */
  private def registerExtractDeps(extract: Extract, depTree: DepTree, env: Map[String, String]): ValidationNel[ConfigError, RuntimeExtract] =
    extract.check match {
      case Some(checkUri) =>
        loadResource(checkUri, env)
          .flatMap(validateResolvedDsos(depTree, extract.name, Echeck, s"extract check ${extract.name} (uri $checkUri)"))
          .map(checkTxt => RuntimeExtract(extract, Some(checkTxt)))
      case None =>
        RuntimeExtract(extract, None).successNel[ConfigError]
    }

  /**
    * Load & parse sql
    * Load & parse pre_check, if specified
    * Load & parse post_check, if specified
    * Check dso dependencies
    */
  private def registerTransformDeps(transform: Transform, depTree: DepTree, env: Map[String, String]): ValidationNel[ConfigError, RuntimeTransform] = {
    // load resources
    val validatedSql = loadResource(transform.sql, env)
      .flatMap(validateResolvedDsos(depTree, transform.name, T, s"Unparsable sql of transform ${transform.name}"))
    val validatedCheck = liftOpt(transform.check)(r => loadResource(r, env)
      .flatMap(validateResolvedDsos(depTree, transform.name, Tcheck, s"Unparsable sql of transform check ${transform.name}")))

    (validatedSql |@| validatedCheck) { (sql, check) => RuntimeTransform(transform, sql, check) }
  }

  private def liftOpt[T1, T2](opt: Option[T1])(toVal: T1 => ValidationNel[ConfigError, T2]): ValidationNel[ConfigError, Option[T2]] =
    opt match {
      case Some(r) => toVal(r).map(Some(_))
      case None => None.successNel[ConfigError]
    }

  private def validateResolvedDsos(depTree: DepTree, name: String, node: ETLNode, errMsgPrefix: String)(contents: String): ValidationNel[ConfigError, String] =
    Try(getDsos(contents)) match {
      case Success(usedDsos) =>
        usedDsos.map(_.toLowerCase).foreach(d => depTree.addActual(d, Node(name, node)))
        contents.successNel[ConfigError]
      case Failure(e: ParseException) =>
        ConfigError(s"$errMsgPrefix: failed to parse, error: ${e.getMessage}").failureNel[String]
      case Failure(e) =>
        ConfigError(s"$errMsgPrefix: failed to parse", Some(e)).failureNel[String]
    }


  private def validateDepTree(depTree: DepTree): ValidationNel[ConfigError, Unit] = {
    val danglingDeps = depTree.dangling
    if (danglingDeps.isEmpty) {
      ().successNel[ConfigError]
    } else {
      val errors = for {
        dangling <- danglingDeps
      } yield {
        val parentType = dangling.parent.`type` match {
          case E => "extract"
          case Echeck => "extract check"
          case T => "transform"
          case Tcheck => "transform check"
          case L => "load"
          case Dangling => "dangling" // should never happen
        }
        ConfigError(s"Unresolved dependency ${dangling.child.id} for ${parentType} ${dangling.parent.id}").failureNel[Unit]
      }
      errors.reduce(_ +++ _)
    }
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
