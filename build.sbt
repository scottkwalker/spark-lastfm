name := """spark-lastfm"""

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"

addCompilerPlugin("org.psywerx.hairyfotr" %% "linter" % "0.1.17")

scapegoatVersion := "1.3.0"

scapegoatDisabledInspections := Seq("RedundantFinalModifierOnCaseClass")

wartremoverErrors ++= Warts.allBut(Wart.NonUnitStatements)