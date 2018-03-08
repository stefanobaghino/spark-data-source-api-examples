package me.baghino.spark.examples.datasource.v1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.LoggerFactory

object ExampleRelation {

  val logger = LoggerFactory.getLogger(classOf[ExampleRelation])

}

/**
  * An example relation that reads a "columnar" file formatted as follows:
  * - on the first line, the number of rows `n`
  * - for each column, there is one line with the column name and `n` with the value for each row
  * @param sqlContext
  * @param content
  */
final class ExampleRelation(override val sqlContext: SQLContext, content: IndexedSeq[String]) extends BaseRelation with PrunedScan {

  private val numRows = content(0).toInt
  private val columns = (1 until content.size by (numRows + 1)).map(i => content(i) -> i)

  private def indexesForColumns(requiredColumns: Array[String]): Array[Int] =
    requiredColumns.flatMap(r => columns.find(c => c._1 == r).map(_._2))

  override def schema: StructType =
    StructType(columns.map(c => StructField(c._1, StringType)))

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    ExampleRelation.logger.info(s"buildScan (pruned): ${requiredColumns.mkString(", ")}")
    sqlContext.sparkContext.parallelize((1 to numRows).map(i => Row(indexesForColumns(requiredColumns).map(_ + i).map(content): _*)))
  }

}
