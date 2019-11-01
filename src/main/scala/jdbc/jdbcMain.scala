package jdbc

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author dalizu on 2019/11/1.
  * @version v1.0
  */
class jdbcMain {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("testMysqlToHiveJdbc")
      .setMaster("local")
    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val url = "jdbc:mysql://mysqlHost:3306/database"
    val tableName = "table"

    // 设置连接用户&密码
    val prop = new java.util.Properties
    prop.setProperty("user","username")
    prop.setProperty("password","pwd")

    // 取得该表数据
    val jdbcDF = spark.read.jdbc(url,tableName,prop)

    jdbcDF.createOrReplaceTempView("test")
    spark.sql("select * from test").show()



  }


}
