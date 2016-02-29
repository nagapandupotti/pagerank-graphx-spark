name := "Assign2_Universities"

version := "1.0"

scalaVersion := "2.10.6"

exportJars := true

mainClass in(Compile, run) := Some("topuniversities")
mainClass in(Compile, packageBin) := Some("topuniversities")

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.6.0",
    "org.apache.spark" %% "spark-graphx" % "1.6.0"
)


