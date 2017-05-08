package spark_etl.util

import java.io.File

object Files {
  def pwd = new File(".").getCanonicalPath
}
