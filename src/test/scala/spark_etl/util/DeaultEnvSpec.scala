package spark_etl.util

import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Inside, Matchers}

class DeaultEnvSpec extends FlatSpec with Matchers with Inside {
  "DefaultEnv" should "obtain dates for start of epoch" in {
    DefaultEnv.getAll(new DateTime(0)).toList should contain allElementsOf Seq(
      // t-1d
      "yyyy-MM-1d"        -> "1969-12",
      "yyyy-MM-dd-1d"     -> "1969-12-31",
      "sod-1d"            -> "1969-12-31 00:00:00",
      "eod-1d"            -> "1969-12-31 23:59:59",
      "y-1d"              -> "1969",
      "m-1d"              -> "12",
      "d-1d"              -> "31",
      // utc-1d
      "utc-yyyy-MM-1d"    -> "1969-12",
      "utc-yyyy-MM-dd-1d" -> "1969-12-31",
      "utc-sod-1d"        -> "1969-12-31 00:00:00",
      "utc-eod-1d"        -> "1969-12-31 23:59:59",
      "utc-y-1d"          -> "1969",
      "utc-m-1d"          -> "12",
      "utc-d-1d"          -> "31"
    )
  }
}
