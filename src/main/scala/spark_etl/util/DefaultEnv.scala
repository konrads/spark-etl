package spark_etl.util

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

/**
  * Default env vars, for token substitution in queries/paths.
  */
object DefaultEnv {
  private val `yyyy-MM` = DateTimeFormat.forPattern("yyyy-MM")
  private val `yyyy-MM-dd` = DateTimeFormat.forPattern("yyyy-MM-dd")
  private val `yyyy-MM-dd HH:mm:ss` = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

  def getAll(date: DateTime, prefix: String = ""): Map[String, String] = {
    val `t-1` = date.minusDays(1)
    val `utc-1` = date.withZone(DateTimeZone.UTC).minusDays(1)
    get(`t-1`, prefix, "-1d") ++
      get(`utc-1`, prefix + "utc-", "-1d")

  }

  def get(date: DateTime, prefix: String = "", suffix: String = ""): Map[String, String] = {
    Map(
      "yyyy-MM"    -> `yyyy-MM`.print(date),
      "yyyy-MM-dd" -> `yyyy-MM-dd`.print(date),
      "sod"        -> `yyyy-MM-dd HH:mm:ss`.print(date.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)),
      "eod"        -> `yyyy-MM-dd HH:mm:ss`.print(date.withHourOfDay(23).withMinuteOfHour(59).withSecondOfMinute(59).withMillisOfSecond(999)),
      "y"          -> date.getYear.toString,
      "m"          -> date.getMonthOfYear.toString,
      "d"          -> date.getDayOfMonth.toString
    ).map { case (k,v) => s"$prefix$k$suffix" -> v }
  }
}
