name := "_____"

version := "1.0"

scalaVersion := "2.10.6"

exportJars := true

mainClass in(Compile, run) := Some("GraphXPageRank")
mainClass in(Compile, packageBin) := Some("GraphXPageRank")

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.6.0",
    "org.apache.spark" %% "spark-graphx" % "1.6.0"
)


