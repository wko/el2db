

organization := "de.tu-dresden"

name := "EL2DB"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.12.6"

resolvers += Classpaths.typesafeReleases

libraryDependencies ++= Seq(
  "org.phenoscape" %% "scowl" % "1.3",
  "net.sourceforge.owlapi" %  "owlapi-distribution"    % "4.2.7",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "org.slf4j" % "slf4j-simple" % "1.7.26" % Test,
  "org.postgresql" % "postgresql" % "9.3-1100-jdbc4",
  "org.scalikejdbc" %% "scalikejdbc"       % "3.3.2",
  "com.typesafe.slick" %% "slick" % "3.2.0",
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.semanticweb.elk" %  "elk-reasoner"    % "0.4.3",
  "org.semanticweb.elk" %  "elk-owlapi"    % "0.4.3"
)

