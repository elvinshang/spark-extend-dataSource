package com.fayayo.excel.spark.sql.datasource

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by rana on 29/9/16.
  */
object ExcelApp extends App {
  println("Application started...")

  val conf = new SparkConf().setAppName("spark-custom-datasource")
  val spark = SparkSession.builder().config(conf).master("local").getOrCreate()

  val df = spark.sqlContext.read.format("com.fayayo.excel.spark.sql.datasource").load("F:\\Excel\\demo03.xlsx")


  df.createOrReplaceTempView("test")
  spark.sql("select * from test").show()


  //创建Properties存储数据库相关属性
  val prop = new Properties()
  prop.put("user", "root")
  prop.put("password", "root123")
  df.write.mode("append").jdbc("jdbc:mysql://localhost:3306/test", "test.abc", prop)

  println("Application Ended...")
}
