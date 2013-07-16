import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

  val appName         = "M2MPWeb"
  val appVersion      = "1.0"

  val appDependencies = Seq(
    // Add your project dependencies here,
    javaCore,
    javaJdbc,
    javaEbean,
    "org.m2mp" % "org.m2mp.db" % "1.0-SNAPSHOT"
  )

  //resolvers += "local2" at "file:///home/florent/.m2/repository"

  val main = play.Project(appName, appVersion, appDependencies).settings(defaultScalaSettings:_*).settings(
    // Add your own project settings here
    resolvers += "local2" at "file:///home/florent/.m2/repository"
  )

}
