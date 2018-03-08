package me.baghino.spark.examples.datasource.v1

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.asScalaIteratorConverter

object ExampleRelationProvider {

  private[ExampleRelationProvider] val logger = LoggerFactory.getLogger(classOf[ExampleRelationProvider])

}

final class ExampleRelationProvider extends RelationProvider with CreatableRelationProvider with DataSourceRegister {

  override def shortName(): String = "example"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {

    ExampleRelationProvider.logger.info("createRelation (read-only)")

    val content = Files.newBufferedReader(Paths.get(parameters("path"))).lines().iterator().asScala.to[Vector]
    new ExampleRelation(sqlContext, content)

  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {

    ExampleRelationProvider.logger.info("createRelation (creatable)")

    import sqlContext.implicits._

    data.cache()
    val content = data.count().toString +: data.columns.map(c => c -> data.select(c).as[String]).flatMap(p => p._1 +: p._2.collect())
    data.unpersist()

    Files.write(Paths.get(parameters("path")), content.mkString(System.lineSeparator()).getBytes)

    new ExampleRelation(sqlContext, content)

  }

}
