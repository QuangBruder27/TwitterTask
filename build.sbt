name := "TwitterBelegAufgabe"

version := "0.1"
scalaVersion := "2.12.13"
val SparkVersion = "2.4.7"
parallelExecution in Test := false
run := Defaults.runTask(fullClasspath in Runtime, mainClass in run in Compile, runner in run).evaluated

libraryDependencies ++=Seq(
	"org.apache.spark" %% "spark-core" % SparkVersion,
	"org.apache.spark" %% "spark-sql" % SparkVersion,
	"org.scalactic" %% "scalactic" % "3.2.5",
	"org.scalatest" %% "scalatest" % "3.2.5" % "test")
