name := """spark-lastfm"""

scalaVersion := "2.11.7"

parallelExecution in Test := false // Saw this message:   org.apache.spark.SparkException: Only one SparkContext may be running in this JVM (see SPARK-2243). To ignore this error, set spark.driver.allowMultipleContexts = true. The currently running SparkContext

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"

addCompilerPlugin("org.psywerx.hairyfotr" %% "linter" % "0.1.17")

scapegoatVersion := "1.3.0"

scapegoatDisabledInspections := Seq("RedundantFinalModifierOnCaseClass")

wartremoverErrors ++= Warts.allBut(Wart.NonUnitStatements, Wart.PublicInference, Wart.TraversableOps, Wart.Var,
  ExtraWart.DateFormatPartial,
  ExtraWart.EnumerationPartial,
  ExtraWart.FutureObject,
  ExtraWart.GenMapLikePartial,
  ExtraWart.GenTraversableLikeOps,
  ExtraWart.GenTraversableOnceOps,
  ExtraWart.LegacyDateTimeCode,
  ExtraWart.ScalaGlobalExecutionContext,
  ExtraWart.StringOpsPartial,
  ExtraWart.TraversableOnceOps,
  ExtraWart.UntypedEquality
)

wartremoverWarnings ++= Seq(Wart.TraversableOps, Wart.Var)