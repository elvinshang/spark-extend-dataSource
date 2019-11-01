package com.fayayo.excel.spark.sql.datasource

import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ SQLContext}

/**
  * 读取excel外部数据源
  */
class DefaultSource extends RelationProvider with SchemaRelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }


  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String],
                              schema: StructType): BaseRelation = {

    val path = parameters.get("path")

    path match {
      case Some(p) => new ExcelRDDDatasourceRelation(sqlContext, p, schema)
      case _ => throw new IllegalArgumentException("Path is required for custom-datasource format!!")
    }
  }




}
