package com.fayayo.excel.spark.sql.datasource

import java.io.DataInputStream

import com.monitorjbl.xlsx.StreamingReader
import org.apache.spark.SparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql.types.StructType
import java.util

import org.apache.spark.sql.Row

/**
  * @author dalizu on 2019/11/1.
  * @desc 解析Excel具体实现
  * @version v1.0 
  */
class ExcelUtils {

  var inputStream: DataInputStream = null

  def compute(sc: SparkContext, schema: StructType,
              path: (String, PortableDataStream)): Iterator[org.apache.spark.sql.Row] = {

    val pathName = path._1
    println("pathName:" + pathName)

    val stream = path._2
    inputStream = stream.open()

    //解析excel
    val workbook = StreamingReader.builder.rowCacheSize(100).bufferSize(4096).open(inputStream)

    val sheet = workbook.getSheetAt(0)

    val sheetRs = sheet.iterator()

    resultSetToSparkRows(sheetRs, schema)
  }

  //返回迭代器
  def resultSetToSparkRows(sheetRs: util.Iterator[org.apache.poi.ss.usermodel.Row],
                           schema: StructType): Iterator[org.apache.spark.sql.Row] = {

    new ExcelNextIterator[Row] {


      //遍历结束执行
      override protected def close(): Unit = {
        println("close stream")
        inputStream.close()
      }

      //获取下一条数据
      override protected def getNext(): org.apache.spark.sql.Row = {

        if (sheetRs.hasNext) {

          val r = sheetRs.next()
          import scala.collection.JavaConversions._
          val cells = new util.ArrayList[String]
          //获取每一行数据
          for (c <- r) {
            cells.add(c.getStringCellValue)
          }

          //转为Row
          Row.fromSeq(cells)
        } else {
          //结束标识
          finished = true
          null.asInstanceOf[Row]
        }

      }

    }

  }

}
