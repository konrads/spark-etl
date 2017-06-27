package spark_etl.util

import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Inside, Matchers}

class DeaultEnvSpec extends FlatSpec with Matchers with Inside {
  "DefaultEnv" should "obtain dates for start of epoch" in {
    DefaultEnv.getAll(new DateTime(0)).toList should contain allElementsOf Seq(
      // t-1
      "yyyy-MM-1"        -> "1969-12",
      "yyyy-MM-dd-1"     -> "1969-12-31",
      "sod-1"            -> "1969-12-31 00:00:00",
      "eod-1"            -> "1969-12-31 23:59:59",
      "y-1"              -> "1969",
      "m-1"              -> "12",
      "d-1"              -> "31",
      // utc-1
      "utc-yyyy-MM-1"    -> "1969-12",
      "utc-yyyy-MM-dd-1" -> "1969-12-31",
      "utc-sod-1"        -> "1969-12-31 00:00:00",
      "utc-eod-1"        -> "1969-12-31 23:59:59",
      "utc-y-1"          -> "1969",
      "utc-m-1"          -> "12",
      "utc-d-1"          -> "31"
    )
  }
}
