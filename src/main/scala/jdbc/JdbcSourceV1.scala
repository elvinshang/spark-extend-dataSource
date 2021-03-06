package jdbc

/**
  * @author dalizu on 2020/2/6.
  * @desc
  * @version v1.0 
  */
import java.sql.{DriverManager, ResultSet}

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory


class JdbcSourceV1 extends RelationProvider {

  override def createRelation(sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {

    new JdbcRelationV1(
      parameters("url"),
      parameters("user"),
      parameters("password"),
      parameters("table")
    )(sqlContext.sparkSession)
  }


}


class JdbcRelationV1(url: String,
                      user: String,
                      password: String,
                      table: String)(@transient val sparkSession: SparkSession)
  extends BaseRelation with PrunedFilteredScan {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = StructType(Seq(
    StructField("id", IntegerType),
    StructField("emp_name", StringType),
    StructField("dep_name", StringType),
    StructField("salary", DecimalType(7, 2)),
    StructField("age", DecimalType(3, 0))
  ))

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    new JdbcRDD(sparkSession.sparkContext, requiredColumns, filters, url, user, password, table, schema)

  }

}


class JdbcRDD(
               sc: SparkContext,
               columns: Array[String],
               filters: Array[Filter],
               url: String,
               user: String,
               password: String,
               table: String,
               schema: StructType)
  extends RDD[Row](sc, Nil) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {

    val sqlBuilder = new StringBuilder()
    sqlBuilder.append(s"SELECT ${columns.mkString(", ")} FROM $table")


    filters.foreach(f => println(f.toString))

    val wheres = filters.flatMap {
      case EqualTo(attribute, value) => Some(s"$attribute = '$value'")
      case _ => None
    }

    if (wheres.nonEmpty) {
      sqlBuilder.append(s" WHERE ${wheres.mkString(" AND ")}")
    }

    val sql = sqlBuilder.toString
    logger.info("============================>>>>>>>>>>"+sql)

    val conn = DriverManager.getConnection(url, user, password)
    val stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    stmt.setFetchSize(1000)
    val rs = stmt.executeQuery()

    context.addTaskCompletionListener(_ => conn.close())

    //返回迭代器
    new Iterator[Row] {

      def hasNext: Boolean = rs.next()

      def next: Row = {
        val values = columns.map {
          case "id" => rs.getInt("id")
          case "emp_name" => rs.getString("emp_name")
          case "dep_name" => rs.getString("dep_name")
          case "salary" => rs.getBigDecimal("salary")
          case "age" => rs.getBigDecimal("age")
        }
        Row.fromSeq(values)
      }

    }
  }

  override protected def getPartitions: Array[Partition] = Array(JdbcPartition(0))

}


case class JdbcPartition(idx: Int) extends Partition {
  override def index: Int = idx
}


object JdbcExampleV1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    val df = spark.read
      .format("jdbc.JdbcSourceV1")
      .option("url", "jdbc:mysql://localhost/test")
      .option("user", "root")
      .option("password", "root123")
      .option("table", "employee")
      .load()

    df.printSchema()
    df.show()

    spark.sql(
      """
        |CREATE TEMPORARY VIEW employee
        |USING jdbc.JdbcSourceV1
        |OPTIONS (
        |  url 'jdbc:mysql://localhost/test',
        |  user 'root',
        |  password 'root123',
        |  table 'employee'
        |)
      """.stripMargin)

    val dfSelect = spark.sql("SELECT COUNT(*), AVG(salary) FROM employee WHERE dep_name = 'Management'")

    //val dfSelect = spark.sql("SELECT emp_name FROM employee WHERE dep_name = 'Management'")

    dfSelect.explain(true)
    dfSelect.show()

    spark.stop()
  }
}
