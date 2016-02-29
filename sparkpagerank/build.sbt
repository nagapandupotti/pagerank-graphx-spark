name := "Assign2"

version := "1.0"

scalaVersion := "2.10.6"

exportJars := true

mainClass in(Compile, run) := Some("PagerankSpark")
mainClass in(Compile, packageBin) := Some("PagerankSpark")

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.6.0",
    "org.apache.spark" %% "spark-graphx" % "1.6.0"
)

