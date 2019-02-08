lazy val root = (project in file(".")).
  settings(
    name := "invdel_spark",
    version := "1.0",
    scalaVersion := "2.11.12",
    mainClass in Compile := Some("com.example.InventoryCleanup")
  )

libraryDependencies ++= Seq(
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}