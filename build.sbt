name := "spark-etl"

version := "0.0.1"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-deprecation", "-feature")

coverageMinimum := 60

coverageFailOnMinimum := true

libraryDependencies ++= {
  lazy val sparkVsn     = "2.1.0"
  lazy val scalazVsn    = "7.2.12"
  lazy val scalaTestVsn = "3.0.3"
  Seq(
    // core
    "org.apache.spark" %% "spark-core"        % sparkVsn     % "compile",
    "org.apache.spark" %% "spark-sql"         % sparkVsn     % "compile",
    "org.scalaz"       %% "scalaz-core"       % scalazVsn,
    "net.jcazevedo"    %% "moultingyaml"      % "0.4.0",
    "org.rogach"       %% "scallop"           % "2.0.2",

    // testing
    "org.scalatest"    %% "scalatest"         % scalaTestVsn % "test",
    "org.scalacheck"   %% "scalacheck"        % "1.13.4"     % "test"
  )
}
