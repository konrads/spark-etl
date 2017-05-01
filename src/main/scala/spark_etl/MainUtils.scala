package spark_etl

import java.io.{PrintWriter, StringWriter}

import org.apache.spark.sql.{DataFrame, SparkSession}
import spark_etl.model._

import scala.util.Try
import scalaz.Scalaz._
import scalaz.Validation.FlatMap._
import scalaz._

object MainUtils {
  val log = org.slf4j.LoggerFactory.getLogger(getClass)

  def validateLocal(confUri: String, env: Map[String, String]): Unit =
    withCtx(confUri, env) {
      ctx =>
        val orgExtracts = ctx.runtimeExtracts.map(_.org)
        val extractReaderValidation = ctx.extractReader.checkLocal(orgExtracts)

        val ortTransforms = ctx.runtimeTransforms.map(_.org)
        val loadWriterValidation = ctx.loadWriter.checkLocal(ortTransforms)

        (extractReaderValidation |@| loadWriterValidation) { (_, _) =>
          log.info(
            s"""Local context validated!
               |
               |ExtractReader validated!
               |
               |LoadWriter validated!""".stripMargin)
        }
    }

  def validateRemote(confUri: String, env: Map[String, String]): Unit =
    withCtx(confUri, env) {
      ctx =>
        val orgExtracts = ctx.runtimeExtracts.map(_.org)
        val extractReaderValidation = ctx.extractReader.checkRemote(orgExtracts)

        val ortTransforms = ctx.runtimeTransforms.map(_.org)
        val loadWriterValidation = ctx.loadWriter.checkRemote(ortTransforms)

        (extractReaderValidation |@| loadWriterValidation) { (_, _) =>
          log.info(
            s"""Remote context validated!
               |
               |ExtractReader validated!
               |
               |LoadWriter validated!""".stripMargin)
        }
    }

  def transform(confUri: String, env: Map[String, String], props: Map[String, String], showCounts: Boolean)(implicit spark: SparkSession): Unit =
    withCtx(confUri, env) {
      ctx =>
        for {
          _ <- readExtracts(ctx.extractReader, ctx.allExtracts)
          transformed <- loadTransforms(ctx.runtimeTransforms)
          _ <- runCounts(transformed, showCounts)
          written <- Try {
            val orgTansforms = transformed.map { case (t, df) => (t.org, df) }
            ctx.loadWriter.write(orgTansforms)
          } match {
            case scala.util.Success(_) => ().successNel[ConfigError]
            case scala.util.Failure(e) => ConfigError("Failed to write out transform", Some(e)).failureNel[Unit]
          }
        } yield written
    }

  def extractCheck(confUri: String, env: Map[String, String])(implicit spark: SparkSession) =
    withCtx(confUri, env) {
      ctx =>
        for {
          _ <- readExtracts(ctx.extractReader, ctx.allExtracts)
          _ <- {
            //
            val runnableChecks = ctx.runtimeTransforms.flatMap(_.extracts.map(_.org).collect { case e if e.check.isDefined => (e.name, e.check.get) })
            runAndReport("Extract checks", runnableChecks)
          }
        } yield ()
    }

  def transformCheck(confUri: String, env: Map[String, String], showCounts: Boolean)(implicit spark: SparkSession) =
    withCtx(confUri, env) {
      ctx =>
        for {
          _ <- readExtracts(ctx.extractReader, ctx.allExtracts)
          transformed <- loadTransforms(ctx.runtimeTransforms)
          _ <- runCounts(transformed, showCounts)
          _ <- {
            val runnableChecks = ctx.runtimeTransforms.collect { case t if t.org.check.isDefined => (t.org.name, t.org.check.get) }
            runAndReport("Transform checks", runnableChecks)
          }
        } yield ()
    }

  private def withCtx(confUri: String, env: Map[String, String])(run: (RuntimeContext) => ValidationNel[ConfigError, Unit]): Unit = {
    val validatedCtx = for {
      conf <- Config.load(confUri, env)
      ctx  <- RuntimeContext.load(conf)
    } yield {
      val ctxDesc =
        s"""|Validated runtime context
            |=========================
            |
            |Extracts:
            |${ctx.allExtracts.map(e => s"• ${e.org.name} -> ${e.org.uri}").mkString("\n")}
            |Extract checks:
            |${ctx.allExtracts.flatMap(e => e.org.check.map(checkUri => List(s"• ${e.org.name} -> $checkUri")).getOrElse(Nil)).mkString("\n")}
            |Transforms:
            |${ctx.runtimeTransforms.map(t => s"• ${t.org.name} -> ${t.org.sql}").mkString("\n")}
            |Transform checks:
            |${ctx.runtimeTransforms.flatMap(t => t.org.check.map(checkUri => List(s"• ${t.org.name} -> $checkUri")).getOrElse(Nil)).mkString("\n")}
           """.stripMargin

      log.info(ctxDesc)
      ctx
    }

    validatedCtx.flatMap(run) match {
      case Success(_) =>
        log.info("Success!")
      case Failure(errors) =>
        val errorStr = errors.map(e => e.exc.map(exc => s"• ${e.msg}, exception: $exc\n${stacktrace(exc)}").getOrElse(s"• ${e.msg}")).toList.mkString("\n")
        log.error(s"Failed due to:\n$errorStr")
        System.exit(1)
    }
  }

  private def validateExtractor(extractor: ExtractReader, extracts: Seq[RuntimeExtract]): ValidationNel[ConfigError, Unit] = {
    val orgExtracts = extracts.map(_.org)
    extractor.checkRemote(orgExtracts).map {
      _ =>
        val orgExtracts = extracts.map(_.org)
        log.info(
          s"""|Validated extract paths
              |=======================
              |${orgExtracts.map(e => s"• ${e.name} -> ${e.uri}").mkString("\n")}""".stripMargin)
    }
  }

  private def readExtracts(extractor: ExtractReader, extracts: Seq[RuntimeExtract])(implicit spark: SparkSession): ValidationNel[ConfigError, Unit] = {
    val orgExtracts = extracts.map(_.org)
    Try {
      extractor.read(orgExtracts).foreach {
        case (e, df) =>
          df.createOrReplaceTempView(e.name)
      }
    } match {
      case scala.util.Success(res) => res.successNel[ConfigError]
      case scala.util.Failure(exc) => ConfigError("Failed to load extracts", Some(exc)).failureNel[Unit]
    }
  }

  private def loadTransforms(transforms: Seq[RuntimeTransform])(implicit spark: SparkSession): ValidationNel[ConfigError, Seq[(RuntimeTransform, DataFrame)]] =
    Try {
      transforms.map {
        t =>
          val df = spark.sql(t.sqlRes.contents)
          df.createOrReplaceTempView(t.org.name)
          (t, df)
      }
    } match {
      case scala.util.Success(res) => res.successNel[ConfigError]
      case scala.util.Failure(exc) => ConfigError("Failed to run transforms", Some(exc)).failureNel[Seq[(RuntimeTransform, DataFrame)]]
    }

  private def runCounts(transformsAndDfs: Seq[(RuntimeTransform, DataFrame)], showCounts: Boolean): ValidationNel[ConfigError, Unit] =
    if (! showCounts)
      ().successNel[ConfigError]
    else
      Try {
        val countDescrs = transformsAndDfs.map { case (t, df) => s"• ${t.org.name}: ${df.count}"}
        log.info(s"Transform counts:\n${countDescrs.mkString("\n")}")
      } match {
        case scala.util.Success(_) => ().successNel[ConfigError]
        case scala.util.Failure(exc) => ConfigError("Failed to run counts", Some(exc)).failureNel[Unit]
      }

  protected def runAndReport(desc: String, transformNameAndSql: Seq[(String, String)])(implicit spark: SparkSession): ValidationNel[ConfigError, Unit] =
    Try {
      val outputs = transformNameAndSql.map {
        case (transformName, sql) =>
          val df = spark.sql(sql)
          val fieldDesc = df.take(100).map(r => df.schema.fields zip r.toSeq).flatMap(_.map {
            case (f, value) => s"${f.name} = $value"
          })
          s"$transformName: ${fieldDesc.mkString(", ")}"
      }
      log.info(s"$desc:\n${outputs.mkString(",")}")
    } match {
      case scala.util.Success(res) => res.successNel[ConfigError]
      case scala.util.Failure(exc) => ConfigError(s"Failed to load $desc", Some(exc)).failureNel[Unit]
    }

  private def stacktrace(t: Throwable) = {
    val w = new StringWriter
    t.printStackTrace(new PrintWriter(w))
    w.toString
  }
}
