package spark_etl

import java.io.{PrintWriter, StringWriter}

import org.apache.spark.sql.{DataFrame, SparkSession}
import spark_etl.model._
import spark_etl.parquet.PathValidator

import scala.util.Try
import scalaz.Scalaz._
import scalaz.Validation.FlatMap._
import scalaz._

object MainUtils {
  val log = org.slf4j.LoggerFactory.getLogger(getClass)

  def validateConf(confUri: String): Unit =
    withCtx(confUri)(_ => log.info("Config validated").successNel[ConfigError])

  def validateExtractPaths(confUri: String): Unit =
    withCtx(confUri)(ctx => validateExtracts(ctx.allExtracts))

  def transform(confUri: String, props: Map[String, String], sink: (Map[String, String], Seq[(Transform, DataFrame)]) => Unit)(implicit spark: SparkSession): Unit =
    withCtx(confUri) {
      ctx =>
        for {
          _ <- validateExtracts(ctx.allExtracts)
          _ <- loadExtracts(ctx.allExtracts)
          transformed <- loadTransforms(ctx.runtimeTransforms.map(_.transform))
          sunk <- Try {
              sink(props, transformed)
            } match {
              case scala.util.Success(_) => ().successNel[ConfigError]
              case scala.util.Failure(e) => ConfigError("Failed to write out transform", Some(e)).failureNel[Unit]
            }
        } yield sunk
    }

  def extractCheck(confUri: String)(implicit spark: SparkSession) =
    withCtx(confUri) {
      ctx =>
        for {
          _ <- validateExtracts(ctx.allExtracts)
          _ <- loadExtracts(ctx.allExtracts)
          _ <- {
            val runnableChecks = ctx.runtimeTransforms.flatMap(_.extracts.collect { case e if e.check.isDefined => (e.name, e.check.get) })
            runAndReport("Extract checks", runnableChecks)
          }
        } yield ()
    }

  def transformCheck(confUri: String)(implicit spark: SparkSession) =
    withCtx(confUri) {
      ctx =>
        for {
          _ <- validateExtracts(ctx.allExtracts)
          _ <- loadExtracts(ctx.allExtracts)
          _ <- loadTransforms(ctx.runtimeTransforms.map(_.transform))
          _ <- {
            val runnableChecks = ctx.runtimeTransforms.collect { case t if t.transform.check.isDefined => (t.transform.name, t.transform.check.get) }
            runAndReport("Transform checks", runnableChecks)
          }
        } yield ()
    }

  protected def withCtx(confUri: String)(run: (RuntimeContext) => ValidationNel[ConfigError, Unit]): Unit = {
    val validatedCtx = for {
      conf <- Config.load(confUri)
      ctx  <- RuntimeContext.load(conf)
    } yield {
      val ctxDesc =
        s"""|
            |Loading extracts:
            |${ctx.allExtracts.map(e => s"• ${e.name} -> ${e.uri}").mkString("\n")}
            |
            |Extract-checks:
            |${ctx.allExtracts.flatMap(e => e.check.map(pc => List(s"• ${e.name} -> ${pc.shortDesc}")).getOrElse(Nil)).mkString("\n")}
            |
            |Transforms:
            |${ctx.runtimeTransforms.map(t => s"• ${t.transform.name} -> ${t.transform.sql.shortDesc}").mkString("\n")}
            |
            |Transform-checks:
            |${ctx.runtimeTransforms.flatMap(t => t.transform.check.map(pc => List(s"• ${t.transform.name} -> ${pc.shortDesc}")).getOrElse(Nil)).mkString("\n")}
           """.stripMargin

      log.info(ctxDesc)
      ctx
    }

    validatedCtx.flatMap(run) match {
      case Success(_) =>
        log.info("Success!")
      case Failure(errors) =>
        val errorStr = errors.map(e => e.exc.map(exc => s"${e.msg}, exception: $exc\n${stacktrace(exc)}").getOrElse(e.msg)).toList.mkString("\n- ")
        log.error(s"Failed due to:\n- $errorStr")
        System.exit(1)
    }
  }

  protected def validateExtracts(extracts: Seq[Extract]): ValidationNel[ConfigError, Unit] = {
    val parquetUris = extracts.collect { case Extract(_name, InParquet, uri, _) => uri}
    PathValidator.validate(parquetUris:_*).map(_ => ())
  }

  protected def loadExtracts(extracts: Seq[Extract])(implicit spark: SparkSession): ValidationNel[ConfigError, Unit] =
    Try {
      extracts.foreach {
        e =>
          val df = e.`type` match {
            case InParquet => spark.read.parquet(e.uri)
            case InCSV => spark.read.csv(e.uri)
            case other => throw new Exception(s"Invalid type ${e.`type`} for extract ${e.name}")
          }
          df.createOrReplaceTempView(e.name)
      }
    } match {
      case scala.util.Success(res) => res.successNel[ConfigError]
      case scala.util.Failure(exc) => ConfigError("Failed to load extracts", Some(exc)).failureNel[Unit]
    }

  protected def loadTransforms(transforms: Seq[Transform])(implicit spark: SparkSession): ValidationNel[ConfigError, Seq[(Transform, DataFrame)]] =
    Try {
      transforms.map {
        t =>
          val df = spark.sql(t.sql.contents.get)
          df.createOrReplaceTempView(t.name)
          (t, df)
      }
    } match {
      case scala.util.Success(res) => res.successNel[ConfigError]
      case scala.util.Failure(exc) => ConfigError("Failed to run transforms", Some(exc)).failureNel[Seq[(Transform, DataFrame)]]
    }

  protected def runAndReport(desc: String, transformNameAndSql: Seq[(String, Resource)])(implicit spark: SparkSession): ValidationNel[ConfigError, Unit] =
    Try {
      val outputs = transformNameAndSql.map {
        case (transformName, res) =>
          val df = spark.sql(res.contents.get)
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
