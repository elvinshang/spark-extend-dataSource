package cn.zj.spark.sql.datasource

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by rana on 29/9/16.
  */
object app extends App {
  println("Application started...")

  val conf = new SparkConf().setAppName("spark-custom-datasource")
  val spark = SparkSession.builder().config(conf).master("local").getOrCreate()

  val df = spark.sqlContext.read.format("cn.zj.spark.sql.datasource").load("E:\\Ideljava\\wokspaces\\spark-extend-dataSource\\data\\input1.txt")

  //print the schema
//  df.printSchema()

  //print the data
//  df.show()

  //save the data
//  df.write.options(Map("format" -> "customFormat")).mode(SaveMode.Overwrite).format("io.dcengines.rana.datasource").save("out_custom/")
//  df.write.options(Map("format" -> "json")).mode(SaveMode.Overwrite).format("io.dcengines.rana.datasource").save("out_json/")
//  df.write.mode(SaveMode.Overwrite).format("io.dcengines.rana.datasource").save("out_none/")

  df.select("id").show()

  //select some specific columns
  df.createOrReplaceTempView("test")
  spark.sql("select id, name, salary from test").show()

  //filter data
  df.createOrReplaceTempView("test")
  spark.sql("select * from test where salary = 20000").show()
  //spark.sql("select * from test").show()

  println("Application Ended...")
}
