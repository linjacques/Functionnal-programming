ThisBuild / scalaVersion := "2.12.18"

enablePlugins(AssemblyPlugin)

lazy val root = (project in file("."))
  .settings(
    name := "projet_analyse_jeux_videos",
    fork in run := true,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-sql" % "3.5.1",
      "org.postgresql" % "postgresql" % "42.3.1",
      "com.typesafe" % "config" % "1.4.3"
    ),
    
    assembly / mainClass := Some("example.Hello"),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _ @ _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )
