package org.learnng.spark.ch03

import org.apache.spark.sql.SparkSession

import scala.io.Source

object App {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("GitHub push counter")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val homeDir = System getenv "HOME"
    val inputPath = homeDir + "/Education/scala/Spark_in_Action/spark-in-action/github-archive/2015-03-01-0.json"
    val ghLog = spark.read json inputPath

    val pushes = ghLog filter "type = 'PushEvent'"
    pushes.printSchema

    println(s"All events: ${ghLog.count}")
    println(s"Only pushes: ${pushes.count}")

    pushes show 7

    val grouped = pushes.groupBy("actor.login").count
    grouped show 7

    val ordered = grouped orderBy grouped("count").desc
    ordered show 7

    val empPath = "ghEmployees.json"
    val empSource = Source.fromFile(empPath)
    val employees = Set() ++ {
      for {
        line <- empSource.getLines
      } yield {
        line.trim.split(":")(1)
          .replace("\"", "")
          .replace("}", "")
      }
    }
    println(s"Company employees: $employees")

    val bcEmployees = sc broadcast employees

    import spark.sqlContext.implicits._
    val isEmp: String => Boolean = bcEmployees.value.contains
    val isEmployee = spark.udf register("isEmpUdf", isEmp)
    val filtered = ordered filter isEmployee($"login")
    filtered.show()

    empSource.close()
  }

}
