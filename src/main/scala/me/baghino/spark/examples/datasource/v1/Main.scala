package me.baghino.spark.examples.datasource.v1

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object Main {

  private[this] val logger = LoggerFactory.getLogger(getClass.getCanonicalName.stripSuffix("$"))

  def main(args: Array[String]): Unit = {

    logger.info("starting spark")

    val spark = SparkSession.builder.master("local").appName("data-source-api-v1-example").getOrCreate()

    logger.info("spark started")

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df = spark.read.format("example").load("apache.txt")

    df.createOrReplaceTempView("apache_projects")

    spark.sql(
      s"""SELECT substring(name, 0, 1) AS initial, collect_list(name) AS names, sum(stars) AS stars_sum
         |FROM apache_projects
         |GROUP BY substring(name, 0, 1)
         |ORDER BY stars_sum DESC
         |""".stripMargin).
      show(truncate = false)

    df.
      groupBy(substring($"name", 0, 1).as("initial")).
      agg(collect_list($"name").as("names"), sum($"stars").as("stars_sum")).
      orderBy($"stars_sum".desc).
      write.format("example").save("output.txt")

    spark.read.format("example").load("output.txt").show(truncate = false)

  }

}