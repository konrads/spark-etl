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
        val orgExtracts = ctx.allExtracts.map(_.org)
        val extractReaderValidation = ctx.extractReader.checkLocal(orgExtracts)

        val loadWriterValidation = ctx.loadWriter.checkLocal(ctx.loads)

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
        val orgExtracts = ctx.allExtracts.map(_.org)
        val extractReaderValidation = ctx.extractReader.checkRemote(orgExtracts)

        val loadWriterValidation = ctx.loadWriter.checkRemote(ctx.loads)

        (extractReaderValidation |@| loadWriterValidation) { (_, _) =>
          log.info(
            s"""Remote context validated!
               |
               |ExtractReader validated!
               |
               |LoadWriter validated!""".stripMargin)
        }
    }

  def transformAndLoad(confUri: String, env: Map[String, String], props: Map[String, String], showCounts: Boolean)(implicit spark: SparkSession): Unit =
    withCtx(confUri, env) {
      ctx =>
        for {
          _ <- readExtracts(ctx.extractReader, ctx.allExtracts)
          transformed <- loadTransforms(ctx.allTransforms)
          _ <- runCounts(transformed, showCounts)
          written <- Try {
            val loadsAndDfs = for {
              (t, df) <- transformed
              l <- ctx.allLoads
              l <- if (t.org.name == l.source) List(l) else Nil
            } yield (l, df)
            ctx.loadWriter.write(loadsAndDfs)
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
            val runnableChecks = ctx.allExtracts.map(_.org).collect { case e if e.check.isDefined => (e.name, e.check.get) }
            runAndReport("Extract checks", runnableChecks)
          }
        } yield ()
    }

  def transformCheck(confUri: String, env: Map[String, String], showCounts: Boolean)(implicit spark: SparkSession) =
    withCtx(confUri, env) {
      ctx =>
        for {
          _ <- readExtracts(ctx.extractReader, ctx.allExtracts)
          transformed <- loadTransforms(ctx.allTransforms)
          _ <- runCounts(transformed, showCounts)
          _ <- {
            val runnableChecks = ctx.allTransforms.collect { case t if t.org.check.isDefined => (t.org.name, t.org.check.get) }
            runAndReport("Transform checks", runnableChecks)
          }
        } yield ()
    }

  private def withCtx(confUri: String, env: Map[String, String])(run: (RuntimeContext) => ValidationNel[ConfigError, Unit]): Unit = {
    val validatedCtx = for {
      conf <- Config.load(confUri, env)
      ctx  <- RuntimeContext.load(conf, env)
    } yield {
      val ctxDesc =
        s"""|Validated runtime context
            |=========================
            |
            |Extracts:
            |${toBullets(ctx.allExtracts.map(e => e.org.name -> e.org.uri))}
            |Extract checks:
            |${toBullets(ctx.allExtracts.flatMap(e => e.org.check.map(checkUri => e.org.name -> checkUri)))}
            |Transforms:
            |${toBullets(ctx.allTransforms.map(t => t.org.name -> t.org.sql))}
            |Transform checks:
            |${toBullets(ctx.allTransforms.flatMap(t => t.org.check.map(checkUri => t.org.name -> checkUri)))}
            |Loads:
            |${toBullets(ctx.loads.map(l => l.name -> l.uri))}
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
          val df = spark.sql(t.sqlContents)
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
        val countDescrs = toBullets(transformsAndDfs.map { case (t, df) => t.org.name -> df.count.toString }, ": ")
        log.info(s"Transform counts:\n$countDescrs")
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

  private def toBullets(kvs: Seq[(String, String)], sep: String = " -> ") =
    if (kvs.isEmpty)
      "  NA"
    else {
      val maxKLen = kvs.map(_._1.length).max
      kvs.map { case (k, v) => s"• ${k.padTo(maxKLen, ' ')}$sep$v" }.mkString("\n")
    }
}
