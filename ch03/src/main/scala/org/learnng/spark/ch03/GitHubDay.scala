package org.learnng.spark.ch03

import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
  * Run in terminal:
  * $ spark-submit --class org.learnng.spark.ch03.GitHubDay \
  * --name "GitHub push counter" \
  * --master local[*] ch03-assembly-1.0.0-SNAPSHOT.jar |
  * <path_to_the_input_JSON_file>
  * <path_to_the_employees_file>
  * <path_to_the_output_file>
  * <output_format>
  */
object GitHubDay {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val sc = spark.sparkContext

    val ghLog = spark.read json args(0)

    val pushes = ghLog filter "type = 'PushEvent'"
    pushes.printSchema

    println(s"All events: ${ghLog.count}")
    println(s"Only pushes: ${pushes.count}")

    pushes show 7

    val grouped = pushes.groupBy("actor.login").count
    grouped show 7

    val ordered = grouped orderBy grouped("count").desc
    ordered show 7

    val empSource = Source.fromFile(args(1))
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

    filtered.write.format(args(3)) save args(2)

    empSource.close()
  }

}
