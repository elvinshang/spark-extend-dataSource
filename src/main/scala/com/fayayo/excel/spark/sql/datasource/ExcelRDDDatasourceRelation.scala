package com.fayayo.excel.spark.sql.datasource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}


class ExcelRDDDatasourceRelation(override val sqlContext : SQLContext, path : String, excelSchema : StructType)
  extends BaseRelation with TableScan with Serializable {

  override def schema: StructType = {
    if (excelSchema != null) {
      excelSchema
    } else {
      StructType(
        StructField("a", StringType, false) ::
        StructField("b", StringType, true) ::
        StructField("c", StringType, true) :: Nil
      )
    }
  }

  //真正调用的方法
  override def buildScan(): RDD[Row] = {
    println("TableScan: buildScan called...")

    val rdd=sqlContext.sparkContext.binaryFiles(path)

    rdd.flatMap(x=>{
      new ExcelUtils().compute(sqlContext.sparkContext,schema,x)
    })

  }

}
