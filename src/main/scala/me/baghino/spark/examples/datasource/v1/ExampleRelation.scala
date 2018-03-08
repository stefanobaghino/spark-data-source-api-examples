package me.baghino.spark.examples.datasource.v1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.LoggerFactory

object ExampleRelation {
  private[ExampleRelation] val logger = LoggerFactory.getLogger(classOf[ExampleRelation])
}

/**
  * An example relation that reads a "columnar" file formatted as follows:
  * - on the first line, the number of rows `n`
  * - for each column, there is one line with the column name and `n` with the values for each row
  * - all values are strings
  */
final class ExampleRelation(override val sqlContext: SQLContext, content: IndexedSeq[String])
  extends BaseRelation with PrunedScan {

  private val numRows: Int = content(0).toInt
  private val columnStartingIndexes = 1 until content.size by (numRows + 1)
  private val columnNames = columnStartingIndexes.map(content)
  private val columnIndex = columnNames.zip(columnStartingIndexes).toMap

  override def schema: StructType =
    StructType(columnNames.map(StructField(_, StringType)))

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    ExampleRelation.logger.info("buildScan (requiredColumns: {})", requiredColumns.mkString(", "))
    sqlContext.sparkContext.parallelize(
      (1 to numRows).map(i => Row(requiredColumns.collect(columnIndex).map(_ + i).map(content): _*))
    )
  }

}
