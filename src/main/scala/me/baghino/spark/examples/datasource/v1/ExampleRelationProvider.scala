package me.baghino.spark.examples.datasource.v1

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.asScalaIteratorConverter

object ExampleRelationProvider {
  private[ExampleRelationProvider] val logger = LoggerFactory.getLogger(classOf[ExampleRelationProvider])
}

/**
  * An example relation that reads a "columnar" file formatted as follows:
  * - on the first line, the number of rows `n`
  * - for each column, there is one line with the column name and `n` with the values for each row
  * - all values are strings
  * WARNING: this is meant exclusively to get a sense of how the Data Source API works
  */
final class ExampleRelationProvider extends RelationProvider with CreatableRelationProvider with DataSourceRegister {

  override def shortName(): String = "example"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val content = Files.newBufferedReader(Paths.get(parameters("path"))).lines().iterator().asScala.to[Vector]
    new ExampleRelation(sqlContext, content)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], df: DataFrame): BaseRelation = {
    val array = df.select(df.columns.map(c => df.col(c).cast("string")): _*).collect()
    val count = array.length.toString
    val data = df.columns.zipWithIndex.map { case (column, i) => column -> array.map(_.getString(i)) }
    val content = count +: data.flatMap { case (column, values) => column +: values }
    Files.write(Paths.get(parameters("path")), content.mkString(System.lineSeparator).getBytes)
    new ExampleRelation(sqlContext, content)
  }

}
